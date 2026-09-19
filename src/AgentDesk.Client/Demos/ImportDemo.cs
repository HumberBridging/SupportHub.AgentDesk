using SupportHub.AgentDesk.V1;

namespace AgentDesk.Client.Demos;

/// <summary>
/// STEP 4 · client streaming — the migration from the old helpdesk.
/// Talk a lot, then get ONE summary back.
/// </summary>
public static class ImportDemo
{
    public static async Task RunAsync(AgentDeskService.AgentDeskServiceClient client, string csvPath)
    {
        Console.WriteLine("CLIENT STREAMING · ImportTickets");

        // No request argument: the request IS the stream we are about to write to.
        using var call = client.ImportTickets();

        var lineNumber = 0;

        foreach (var line in File.ReadLines(csvPath))
        {
            lineNumber++;
            if (lineNumber == 1) continue;                      // the CSV header

            var cells = line.Split(',');

            // WriteAsync returns as soon as the row is on its way. We are not
            // waiting for a per-row answer — there isn't one.
            await call.RequestStream.WriteAsync(new ImportTicketRequest
            {
                LineNumber = lineNumber,
                Title = Cell(cells, 0),
                Description = Cell(cells, 1),
                CustomerEmail = Cell(cells, 2),
                Priority = ParsePriority(Cell(cells, 3)),
            });

            Console.WriteLine($"  sent line {lineNumber}");
        }

        // THE LINE EVERYONE FORGETS. Until the client says "that was everything", the server's `await foreach` never ends, and both sides wait politely
        // for each other until the caller gives up. Every developer meets this bug
        await call.RequestStream.CompleteAsync();

        // Now — and only now — the single response arrives.
        var summary = await call.ResponseAsync;

        Console.WriteLine();
        Console.WriteLine($"accepted {summary.Accepted}, rejected {summary.Rejected}, " +
                          $"in {summary.Elapsed.ToTimeSpan().TotalMilliseconds:N0} ms");
        Console.WriteLine($"new ticket ids: {string.Join(", ", summary.CreatedTicketIds)}");

        foreach (var error in summary.Errors)
            Console.WriteLine($"  line {error.LineNumber} rejected: {error.Reason}");

        Console.WriteLine();
        Console.WriteLine("A bad row did not stop the import. Three of the eight rows are broken on purpose.");
    }

    private static string Cell(string[] cells, int index) =>
        index < cells.Length ? cells[index].Trim() : string.Empty;

    private static TicketPriority ParsePriority(string text) => text.ToLowerInvariant() switch
    {
        "low" => TicketPriority.Low,
        "high" => TicketPriority.High,
        "critical" => TicketPriority.Critical,
        _ => TicketPriority.Medium,
    };

}
