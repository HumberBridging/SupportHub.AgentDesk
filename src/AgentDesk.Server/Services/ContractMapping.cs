using Google.Protobuf.WellKnownTypes;
using SupportHub.AgentDesk.Server.Domain;
using SupportHub.AgentDesk.V1;

namespace SupportHub.AgentDesk.Server.Services;

/// <summary>
/// Domain records & generated protobuf messages.
/// The ONLY place that knows about both worlds. It is the gRPC twin of Module 5's
/// TicketDto.From(ticket): never hand your storage type straight to a caller.
/// </summary>
public static class ContractMapping
{
    public static Ticket ToContract(this TicketRecord t)
    {
        var ticket = new Ticket
        {
            TicketId = t.Id,
            Title = t.Title,
            Description = t.Description,
            Status = t.Status.ToContract(),
            Priority = t.Priority.ToContract(),
            CustomerId = t.CustomerId,
            // Timestamp must be UTC. FromDateTimeOffset normalises it for us.
            CreatedAtUtc = Timestamp.FromDateTimeOffset(t.CreatedAtUtc),
        };

        // `optional int32` in the .proto: assign only when there IS a value,
        // otherwise the caller cannot tell "unassigned" from "agent 0".
        if (t.AssignedAgentId is int agentId)
            ticket.AssignedAgentId = agentId;

        // A repeated field is a read-only property: you Add/AddRange, never assign.
        ticket.Tags.AddRange(t.Tags);

        return ticket;
    }

    public static Agent ToContract(this AgentRecord a, int openTickets) => new()
    {
        AgentId = a.Id,
        DisplayName = a.DisplayName,
        IsActive = a.IsActive,
        OpenTicketCount = openTickets,
    };

    // The enum numbers match on both sides, so these could be a cast. Writing
    // them out means that the day the two drift apart, the compiler tells you.
    public static TicketStatus ToContract(this TicketState s) => s switch
    {
        TicketState.Open => TicketStatus.Open,
        TicketState.InProgress => TicketStatus.InProgress,
        TicketState.Resolved => TicketStatus.Resolved,
        TicketState.Closed => TicketStatus.Closed,
        _ => TicketStatus.Unspecified,
    };

    public static TicketPriority ToContract(this Priority p) => p switch
    {
        Priority.Low => TicketPriority.Low,
        Priority.Medium => TicketPriority.Medium,
        Priority.High => TicketPriority.High,
        Priority.Critical => TicketPriority.Critical,
        _ => TicketPriority.Unspecified,
    };
}
