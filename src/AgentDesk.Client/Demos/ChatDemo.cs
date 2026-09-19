using Grpc.Core;
using SupportHub.AgentDesk.V1;

namespace AgentDesk.Client.Demos;

public static class ChatDemo
{
    public static async Task RunAsync(AgentDeskService.AgentDeskServiceClient client,
        int ticketId, string role, string name)
    {
        Console.WriteLine($"BIDIRECTIONAL · Chat on ticket {ticketId} as {name} ({role})");
        Console.WriteLine("Type a message and press Enter. An empty line leaves the room.");
        Console.WriteLine();

        // Who you are travels as METADATA — gRPC's equivalent of an HTTP header —
        // once, with the call, instead of being repeated inside every message.
        var headers = new Metadata
        {
            { "x-ticket-id", ticketId.ToString() },
            { "x-display-name", name },
            { "x-role", role },
        };

        using var cts = new CancellationTokenSource();
        using var call = client.Chat(headers, cancellationToken: cts.Token);

        // READING happens on its own, at the same time as the typing below.
        // Neither loop waits for the other. That is what bidirectional buys you.
        var reading = Task.Run(async () =>
        {
            try
            {
                await foreach (var message in call.ResponseStream.ReadAllAsync(cts.Token))
                {
                    // The room broadcasts to everyone, including me. Skip my own
                    // words; keep the server's (join, leave) announcements.
                    if (message.Role != ChatRole.System && message.Sender == name) continue;

                    var at = message.SentAt.ToDateTimeOffset().ToLocalTime().ToString("HH:mm:ss");
                    Console.WriteLine($"[{at}] {message.Sender}: {message.Text}");
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or RpcException { StatusCode: StatusCode.Cancelled })
            {
                // The call ended.
            }
        });

        // WRITING is just the keyboard. Notice how little we send: the text, and
        // nothing else. The server stamps who said it and when.
        while (Console.ReadLine() is { Length: > 0 } line)
            await call.RequestStream.WriteAsync(new ChatMessage { Text = line });

        await call.RequestStream.CompleteAsync();   // "I have stopped talking"
        await reading;                              // let the last messages land
    }
}
