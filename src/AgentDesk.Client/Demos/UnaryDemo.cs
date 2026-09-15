using SupportHub.AgentDesk.V1;

namespace SupportHub.AgentDesk.Client.Demos;

public static class UnaryDemo
{
    public static async Task GetTicketAsync(AgentDeskService.AgentDeskServiceClient client, int ticketId)
    {
        Console.WriteLine("Get Ticket request received at {0}", DateTime.UtcNow);

        var request = new GetTicketRequest { TicketId = ticketId };

        var ticket = await client.GetTicketAsync(request, deadline:DateTime.UtcNow.AddSeconds(05));

        Console.WriteLine($"Retrieved ticket: {ticket.TicketId}");
    }

    
    public static async Task AssignTicketAsync(AgentDeskService.AgentDeskServiceClient client, int ticketId)
    {
        Console.WriteLine("Assign Ticket request received at {0}", DateTime.UtcNow);

        var request = new AssignTicketRequest { TicketId = ticketId };

        var response = await client.AssignTicketAsync(request, 
            deadline: DateTime.UtcNow.AddSeconds(05));

        Print(response.Ticket);

        Console.WriteLine($"  -> {response.Agent.DisplayName} is now holding {response.Agent.OpenTicketCount} ticket(s).");
        Console.WriteLine();
    }

    //Helper method to print ticket details
    private static void Print(Ticket ticket)
    {
        // HasAssignedAgentId only exists because the field is `optional` in the
        // .proto. That is how we tell "nobody has it" from "agent 0".
        var owner = ticket.HasAssignedAgentId ? $"agent {ticket.AssignedAgentId}" : "unassigned";
        var tags = ticket.Tags.Count > 0 ? $"  [{string.Join(", ", ticket.Tags)}]" : string.Empty;

        Console.WriteLine($"  #{ticket.TicketId}  {ticket.Title}");
        Console.WriteLine($"      {ticket.Status} · {ticket.Priority} · customer {ticket.CustomerId} · {owner}{tags}");
        Console.WriteLine($"      created {ticket.CreatedAtUtc.ToDateTimeOffset().ToLocalTime():yyyy-MM-dd HH:mm}");
    }
}
