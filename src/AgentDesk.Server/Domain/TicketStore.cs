using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;

namespace SupportHub.AgentDesk.Server.Domain;

/// <summary>
/// In-memory stand-in for the SupportHub database from Module 5 — the same
/// customers, agents and tickets, seeded from the same SeedData. Registered as a
/// singleton, so every mutation happens under one lock.
/// </summary>
public sealed class TicketStore
{
    private readonly Lock _gate = new();
    private readonly Dictionary<int, TicketRecord> _tickets = new();
    private readonly List<AgentRecord> _agents;
    private readonly List<CustomerRecord> _customers;

    /// <summary>
    /// How many in-flight tickets one agent may hold. This is AgentDesk's ROUTING
    /// POLICY, not a database column: the REST API has no idea it exists. Deciding
    /// who can take work is exactly the sort of rule a routing service owns.
    /// </summary>
    public const int MaxOpenTicketsPerAgent = 3;

    public TicketStore()
    {
        // Production note: take a TimeProvider in the constructor instead of
        // reading the clock directly, and tests can control time. Left out here
        // so the seed data reads like the seed data.
        var now = DateTimeOffset.UtcNow;

        // The same two customers and two agents as SupportHub's SeedData...
        _customers =
        [
            new(1, "Acme Logistics", "ops@acme.example"),
            new(2, "Globex Retail", "it@globex.example"),
        ];

        // ...plus one inactive agent, so you can watch IsActive being honoured.
        _agents =
        [
            new(1, "Priya N.", IsActive: true),
            new(2, "Tom R.", IsActive: true),
            new(3, "Ana K.", IsActive: false),
        ];

        // Tickets 1 and 2 are SupportHub's seeded tickets, field for field.
        Seed(new(1, "Invoice 4471 shows the wrong tax rate",
            "GST is being applied at 13% instead of 5% on Alberta shipments.",
            TicketState.Open, Priority.High, CustomerId: 1, AssignedAgentId: 1,
            now.AddMinutes(-95), ["billing"]));

        Seed(new(2, "Cannot log in after password reset",
            "Reset email arrives, new password is rejected as invalid.",
            TicketState.Open, Priority.Critical, CustomerId: 2, AssignedAgentId: null,
            now.AddMinutes(-40), ["bug", "urgent"]));

        // The rest are AgentDesk's own demo data: one ticket per status, so every
        // rule below has something to bite on.
        Seed(new(3, "Shipment tracking page times out",
            "The tracking page spins for 30 seconds and then shows an empty table.",
            TicketState.InProgress, Priority.High, CustomerId: 1, AssignedAgentId: 1,
            now.AddMinutes(-30), ["bug"]));

        Seed(new(4, "Duplicate charge on order 8823",
            "The customer was charged twice on the same day.",
            TicketState.Resolved, Priority.Medium, CustomerId: 2, AssignedAgentId: 2,
            now.AddHours(-6), ["billing"]));

        Seed(new(5, "Export to CSV is missing the tax column",
            "Fixed in last week's release.",
            TicketState.Closed, Priority.Low, CustomerId: 1, AssignedAgentId: null,
            now.AddDays(-3), []));
    }

    public bool TryGet(int ticketId, [MaybeNullWhen(false)] out TicketRecord ticket)
    {
        lock (_gate) return _tickets.TryGetValue(ticketId, out ticket);
    }

    /// <summary>How much work an agent is holding right now (Open or InProgress).</summary>
    public int OpenTicketsFor(int agentId)
    {
        lock (_gate) return CountOpenFor(agentId);
    }

    /// <summary>
    /// Routes an Open ticket to the least-busy active agent and moves it to
    /// InProgress — the one transition the status machine allows from Open.
    /// </summary>
    public (TicketRecord Ticket, AgentRecord Agent) Assign(int ticketId)
    {
        TicketRecord updated;
        AgentRecord agent;

        lock (_gate)
        {
            if (!_tickets.TryGetValue(ticketId, out var ticket))
                throw new TicketNotFoundException(ticketId);

            if (ticket.AssignedAgentId is int already)
                throw new TicketStateException($"Ticket {ticket.Id} is already assigned to agent {already}.");

            // The request is fine; the ticket's state isn't.
            if (!ticket.CanTransitionTo(TicketState.InProgress))
                throw new TicketStateException(
                    $"A ticket in {ticket.Status} cannot be assigned. Allowed: Open -> InProgress.");

            agent = _agents
                .Where(a => a.IsActive)
                .Select(a => (Agent: a, Load: CountOpenFor(a.Id)))
                .Where(x => x.Load < MaxOpenTicketsPerAgent)
                .OrderBy(x => x.Load)
                .ThenBy(x => x.Agent.Id)
                .Select(x => x.Agent)
                .FirstOrDefault()
                ?? throw new NoAgentAvailableException(
                    $"Every active agent is holding {MaxOpenTicketsPerAgent} tickets already.");

            updated = ticket with
            {
                Status = TicketState.InProgress,
                AssignedAgentId = agent.Id,
            };
            _tickets[ticket.Id] = updated;
        }

        Publish(new TicketEvent(TicketEventKind.Assigned, updated, agent, DateTimeOffset.UtcNow));
        return (updated, agent);
    }

    /// <summary>Everything still being worked on: Open or InProgress, oldest first.</summary>
    public IReadOnlyList<TicketRecord> LiveTickets()
    {
        lock (_gate)
        {
            return _tickets.Values
                .Where(IsLive)
                .OrderBy(t => t.CreatedAtUtc)
                .ToList();
        }
    }

    // ---- live watchers -------------------------------------------------------
    // One channel per watcher. Assign() and Create() write to all of them; each
    // WatchQueue stream reads from its own. This is the whole fan-out mechanism.

    private readonly ConcurrentDictionary<Guid, Channel<TicketEvent>> _watchers = new();

    /// <summary>
    /// Subscribes to every future ticket event. Dispose the subscription to stop.
    /// The channel is BOUNDED: a slow watcher drops its oldest events rather than
    /// making the server buffer without limit. That is back-pressure.
    /// </summary>
    public Subscription Subscribe()
    {
        var channel = Channel.CreateBounded<TicketEvent>(new BoundedChannelOptions(capacity: 100)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });

        var id = Guid.NewGuid();
        _watchers[id] = channel;

        return new Subscription(channel.Reader, () =>
        {
            if (_watchers.TryRemove(id, out var removed)) removed.Writer.TryComplete();
        });
    }

    public int WatcherCount => _watchers.Count;

    public sealed class Subscription(ChannelReader<TicketEvent> events, Action onDispose) : IDisposable
    {
        public ChannelReader<TicketEvent> Events { get; } = events;

        public void Dispose() => onDispose();
    }

    private void Publish(TicketEvent evt)
    {
        foreach (var watcher in _watchers.Values)
            watcher.Writer.TryWrite(evt);
    }

    // ---- helpers -------------------------------------------------------------

    private static bool IsLive(TicketRecord t) =>
        t.Status is TicketState.Open or TicketState.InProgress;

    private int CountOpenFor(int agentId) =>
        _tickets.Values.Count(t => t.AssignedAgentId == agentId && IsLive(t));

    private void Seed(TicketRecord ticket) => _tickets[ticket.Id] = ticket;
}
