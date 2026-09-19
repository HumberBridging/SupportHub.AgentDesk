namespace SupportHub.AgentDesk.Server.Domain;

// -----------------------------------------------------------------------------
// The DOMAIN model — deliberately NOT the generated protobuf classes.
//
// These records mirror the Module 5 entities (Models/Ticket.cs, Models/Agent.cs,
// Models/Enums.cs) because AgentDesk stands in for the same system: same int
// ids, same enum numbering, same status machine. Only the storage differs — the
// REST API has EF Core and SQLite, AgentDesk keeps it in memory.
//
// Keeping these separate from the wire types means we can change the rules
// without breaking callers, and publish a v2 contract without rewriting the
// rules. The translation lives in Services/ContractMapping.cs.
// -----------------------------------------------------------------------------

// Numbering matches SupportHub.Api.Models.TicketStatus / TicketPriority exactly.
public enum TicketState { Open = 1, InProgress = 2, Resolved = 3, Closed = 4 }

public enum Priority { Low = 1, Medium = 2, High = 3, Critical = 4 }

public sealed record TicketRecord(
    int Id,
    string Title,
    string Description,
    TicketState Status,
    Priority Priority,
    int CustomerId,
    int? AssignedAgentId,
    DateTimeOffset CreatedAtUtc,
    IReadOnlyList<string> Tags)
{
    /// <summary>
    /// The same status machine as the REST API: Open -> InProgress -> Resolved
    /// -> Closed, one step forward at a time.
    /// </summary>
    public bool CanTransitionTo(TicketState next) => (Status, next) switch
    {
        (TicketState.Open, TicketState.InProgress) => true,
        (TicketState.InProgress, TicketState.Resolved) => true,
        (TicketState.Resolved, TicketState.Closed) => true,
        _ => false,
    };
}

public sealed record AgentRecord(int Id, string DisplayName, bool IsActive);

public sealed record CustomerRecord(int Id, string Name, string Email);

/// <summary>Something happened to a ticket. WatchQueue turns these into QueueEvents.</summary>
public enum TicketEventKind { Assigned, Created }

public sealed record TicketEvent(TicketEventKind Kind, TicketRecord Ticket, AgentRecord? Agent, DateTimeOffset OccurredAt);

// Business-rule failures. The gRPC layer translates these into status codes, so
// the domain never needs to know it is being called over gRPC.
public sealed class TicketNotFoundException(int ticketId)
    : Exception($"Ticket {ticketId} does not exist.");

public sealed class TicketStateException(string message) : Exception(message);

public sealed class NoAgentAvailableException(string message) : Exception(message);

//Client Streaming
public sealed class InvalidImportRowException(string message) : Exception(message);

public sealed class UnknownCustomerException(string email)
    : Exception($"No customer with email '{email}'.");
