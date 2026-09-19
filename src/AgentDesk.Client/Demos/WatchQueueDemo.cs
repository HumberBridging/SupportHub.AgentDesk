using Grpc.Core;
using SupportHub.AgentDesk.V1;
using System.Collections.Concurrent;

namespace SupportHub.AgentDesk.Client.Demos;

/// <summary>
/// Server streaming — the live queue board on the agents' wall.
/// Ask once, then listen. Nobody polls.
///
/// Run this in one terminal and `dotnet run -- assign 2` in another.
/// </summary>
public static class WatchQueueDemo
{
    public static async Task RunAsync(AgentDeskService.AgentDeskServiceClient client)
    {
        Console.WriteLine("SERVER STREAMING · WatchQueue — Ctrl+C to stop");

        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        // A streaming call returns IMMEDIATELY — there is nothing to await here.
        // What you await is each message arriving on call.ResponseStream.
        using var call = client.WatchQueue(new WatchQueueRequest(), cancellationToken: cts.Token);

        try
        {
            await foreach (var evt in call.ResponseStream.ReadAllAsync(cts.Token))
            {
                var at = evt.OccurredAt.ToDateTimeOffset().ToLocalTime().ToString("HH:mm:ss");

                // A `oneof` becomes a PayloadCase enum in C#. Exactly one branch
                // can ever match, and the compiler keeps us honest about which.
                switch (evt.PayloadCase)
                {
                    case QueueEvent.PayloadOneofCase.Backlog:
                        Console.WriteLine($"[{at}] backlog   #{evt.Backlog.TicketId}  {evt.Backlog.Title}  ({evt.Backlog.Status}, {evt.Backlog.Priority})");
                        break;

                    case QueueEvent.PayloadOneofCase.Assigned:
                        Console.WriteLine($"[{at}] ASSIGNED  #{evt.Assigned.Ticket.TicketId} -> {evt.Assigned.Agent.DisplayName}");
                        break;

                    case QueueEvent.PayloadOneofCase.Created:
                        Console.WriteLine($"[{at}] NEW       #{evt.Created.TicketId}  {evt.Created.Title}  ({evt.Created.Priority})");
                        break;
                }
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or RpcException { StatusCode: StatusCode.Cancelled })
        {
            // Ctrl+C. The cancellation travelled to the server too — its log says
            // "Watcher left" at the same moment.
            Console.WriteLine("Stopped watching.");
        }
    }
}
