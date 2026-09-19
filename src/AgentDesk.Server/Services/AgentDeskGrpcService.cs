using AgentDesk.Server.Services;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using SupportHub.AgentDesk.Server.Domain;
using SupportHub.AgentDesk.V1;
using System.Diagnostics;

namespace SupportHub.AgentDesk.Server.Services;

public sealed class AgentDeskGrpcService : AgentDeskService.AgentDeskServiceBase
{
    private readonly TicketStore _store;
    private readonly ILogger<AgentDeskGrpcService> _logger;

    private readonly ChatRoomRegistry _chatRooms;

    private static readonly TimeProvider _clock = TimeProvider.System;
    public AgentDeskGrpcService(TicketStore ticketStore, ILogger<AgentDeskGrpcService> logger, TimeProvider clock, ChatRoomRegistry chatRooms)
    {
        _store = ticketStore;
        _logger = logger;
        _chatRooms = chatRooms;
    }

    //Unary
    public override Task<Ticket> GetTicket(GetTicketRequest request, ServerCallContext context)
    {
        //Validation
        var ticketId = RequireTicketId(request.TicketId);

        if(!_store.TryGet(ticketId, out var ticket))
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"Ticket {ticketId} not found."));
        }

        Console.WriteLine("Get Ticket request received at {0}. Ticket ID: {1}", DateTime.UtcNow, ticketId);

        return Task.FromResult(ticket.ToContract());
    }

    public override Task<AssignTicketResponse> AssignTicket(AssignTicketRequest request, ServerCallContext context)
    {
        var ticketId = RequireTicketId(request.TicketId);

        _logger.LogInformation("Routing ticket {TicketId}", ticketId);

        try
        {
            var (ticket, agent) = _store.Assign(ticketId);

            return Task.FromResult(new AssignTicketResponse
            {
                Ticket = ticket.ToContract(),
                Agent = agent.ToContract(_store.OpenTicketsFor(agent.Id)),
            });
        }

        // callers build their retry and error handling on these, so they are part of the contract.
        catch (TicketNotFoundException ex)
        {
            // "There is no such ticket."
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (TicketStateException ex)
        {
            // "The request is fine; the ticket is in the wrong state." Retrying
            // the identical call will not help until something else changes.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (NoAgentAvailableException ex)
        {
            // "Right request, wrong moment." This one IS worth retrying later.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }
    }

    //Server Streaming
    public override async Task WatchQueue(WatchQueueRequest request,
    IServerStreamWriter<QueueEvent> responseStream, ServerCallContext context)
    {
        // Subscribe BEFORE sending the backlog, or an event that happens in
        // between the two would be missed by this watcher.
        using var subscription = _store.Subscribe();

        _logger.LogInformation("Watcher joined — {Count} watching", _store.WatcherCount);

        try
        {
            // 1. Catch the client up: everything that is already open.
            foreach (var ticket in _store.LiveTickets())
            {
                await responseStream.WriteAsync(new QueueEvent
                {
                    OccurredAt = Now(),
                    Backlog = ticket.ToContract(),
                });
            }

            // 2. Then hold the stream open and write each new event as it happens.
            //    Passing the call's CancellationToken is what makes the server stop
            //    working the moment the client hangs up.
            await foreach (var evt in subscription.Events.ReadAllAsync(context.CancellationToken))
                await responseStream.WriteAsync(ToQueueEvent(evt));
        }
        catch (OperationCanceledException)
        {
            // The client hung up. For a watch, that IS the normal ending.
        }

        _logger.LogInformation("Watcher left");

        // A production version would also: filter the stream (by agent, by minimum
        // priority) from fields on WatchQueueRequest, reject a bad filter with
        // InvalidArgument or NotFound, and send a periodic Heartbeat so that proxies
        // and load balancers do not cut a stream that has simply been quiet.
    }

    //Client Streaming
    public override async Task<ImportTicketsResponse> ImportTickets(IAsyncStreamReader<ImportTicketRequest> requestStream, ServerCallContext context)
    {
        var started = Stopwatch.GetTimestamp();
        var result = new ImportTicketsResponse();

        // Each row is handled the moment it lands: the server never holds the whole
        // file in memory. That is the difference between importing two thousand
        // tickets and importing two million.
        //
        // This loop ends when the client calls RequestStream.CompleteAsync().
        await foreach (var row in requestStream.ReadAllAsync(context.CancellationToken))
        {
            try
            {
                var ticket = ImportOne(row);
                result.Accepted++;
                result.CreatedTicketIds.Add(ticket.Id);
            }
            catch (Exception ex) when (ex is InvalidImportRowException or UnknownCustomerException)
            {
                // One bad row is not a failed import: record it and keep going.
                result.Rejected++;
                result.Errors.Add(new ImportError { LineNumber = row.LineNumber, Reason = ex.Message });
            }
        }

        result.Elapsed = Duration.FromTimeSpan(Stopwatch.GetElapsedTime(started));
        _logger.LogInformation("Import finished: {Accepted} accepted, {Rejected} rejected",
            result.Accepted, result.Rejected);

        return result;
    }

    //Bi-directional streaming
    public override async Task Chat(IAsyncStreamReader<ChatMessage> requestStream, IServerStreamWriter<ChatMessage> responseStream, ServerCallContext context)
    {
        // Who is calling? Identity travels in METADATA — gRPC's equivalent of an
        // HTTP header — not inside every message. (In production it would come
        // from an authenticated token rather than a header the caller types, but
        // the mechanism is exactly this.)
        var headers = context.RequestHeaders;
        var name = headers.GetValue("x-display-name")?.Trim();
        var role = headers.GetValue("x-role")?.Trim().ToLowerInvariant() switch
        {
            "agent" => ChatRole.Agent,
            "customer" => ChatRole.Customer,
            _ => ChatRole.Unspecified,
        };

        if (!int.TryParse(headers.GetValue("x-ticket-id"), out var ticketId) ||
            string.IsNullOrEmpty(name) || role == ChatRole.Unspecified)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                "Chat needs metadata: x-ticket-id (a number), x-display-name and x-role (agent | customer)."));
        }

        if (!_store.TryGet(ticketId, out _))
            throw new RpcException(new Status(StatusCode.NotFound, $"Ticket {ticketId} does not exist."));

        var room = _chatRooms.GetOrCreate(ticketId);
        var me = room.Join();

        // TWO LOOPS RUN AT THE SAME TIME. That is the whole point of bidirectional:
        // reading and writing are independent, so neither side has to take turns.
        //
        // OUTBOUND loop: drain my inbox onto the response stream. This is the only
        // code that writes to responseStream — a gRPC stream allows one writer.
        var outbound = PumpInboxAsync(me, responseStream, context.CancellationToken);

        room.Broadcast(SystemMessage(ticketId, $"{name} joined — {room.Count} in the room."));
        _logger.LogInformation("{Name} joined the chat on ticket {TicketId}", name, ticketId);

        try
        {
            // INBOUND loop: whatever this client types, for as long as they type it.
            await foreach (var incoming in requestStream.ReadAllAsync(context.CancellationToken))
            {
                if (string.IsNullOrWhiteSpace(incoming.Text)) continue;

                // Stamp sender, role, ticket and time OURSELVES, and ignore whatever
                // the client put in those fields. A chat where the client gets to
                // claim who it is is not a chat you would ship.
                room.Broadcast(new ChatMessage
                {
                    Text = incoming.Text.Trim(),
                    TicketId = ticketId,
                    Sender = name,
                    Role = role,
                    SentAt = Now(),
                });
            }
            // Getting here means the client called RequestStream.CompleteAsync().
        }
        catch (OperationCanceledException)
        {
            // The client vanished without saying goodbye.
        }
        finally
        {
            room.Broadcast(SystemMessage(ticketId, $"{name} left."));
            me.Dispose();       // completes my inbox, so the outbound loop drains and ends
            _logger.LogInformation("{Name} left the chat on ticket {TicketId}", name, ticketId);
        }

        try { await outbound; }
        catch (OperationCanceledException) { /* the caller has gone; nothing left to deliver */ }

        // A production version would also: check that this caller is allowed on
        // this ticket, persist the transcript, and let the SERVER start messages of
        // its own — an SLA countdown only the agent can see, for example. That last
        // one is what makes this a conversation instead of a series of replies.

    }

    //Helpers
    private static int RequireTicketId(int ticketId) => ticketId <= 0 ? 
        throw new RpcException(new Status(StatusCode.InvalidArgument, "ticket_id must be greater than zero.")): ticketId;

    private QueueEvent ToQueueEvent(TicketEvent evt)
    {
        var queueEvent = new QueueEvent { OccurredAt = Timestamp.FromDateTimeOffset(evt.OccurredAt) };

        // Setting one field of a `oneof` automatically clears the others, so the
        // client's switch on PayloadCase can only ever land on one branch.
        switch (evt.Kind)
        {
            case TicketEventKind.Assigned:
                queueEvent.Assigned = new TicketAssigned
                {
                    Ticket = evt.Ticket.ToContract(),
                    Agent = evt.Agent!.ToContract(_store.OpenTicketsFor(evt.Agent.Id)),
                };
                break;
            //Server streaming
            case TicketEventKind.Created:
                queueEvent.Created = evt.Ticket.ToContract();
                break;
        }

        return queueEvent;
    }

    private static Timestamp Now() => Timestamp.FromDateTimeOffset(_clock.GetUtcNow());

    //Client Streaming
    private TicketRecord ImportOne(ImportTicketRequest row)
    {
        if (string.IsNullOrWhiteSpace(row.Title))
            throw new InvalidImportRowException("Title is required.");

        // The legacy export knows customers by email; SupportHub knows them by id.
        // Resolving that is the import's job.
        var customer = _store.FindCustomerByEmail(row.CustomerEmail)
            ?? throw new UnknownCustomerException(row.CustomerEmail);

        return _store.Create(row.Title, row.Description, customer.Id, row.Priority.ToDomain());
    }

    //Bi-Directional
    private static async Task PumpInboxAsync(ChatRoom.Membership me,
        IServerStreamWriter<ChatMessage> responseStream, CancellationToken cancellationToken)
    {
        await foreach (var message in me.Inbox.ReadAllAsync(cancellationToken))
            await responseStream.WriteAsync(message);
    }

    private static ChatMessage SystemMessage(int ticketId, string text) => new()
    {
        Text = text,
        TicketId = ticketId,
        Sender = "SupportHub",
        Role = ChatRole.System,
        SentAt = Now(),
    };
}
