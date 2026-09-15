using Grpc.Core;
using Grpc.Net.Client;
using SupportHub.AgentDesk.V1;
using SupportHub.AgentDesk.Client.Demos;

// Point it somewhere else with environment variables, e.g. Azure Container Apps:
//   AGENTDESK_GRPC_URL=https://agentdesk.<env-id>.canadacentral.azurecontainerapps.io
var grpcUrl = Environment.GetEnvironmentVariable("AGENTDESK_GRPC_URL") ?? "http://localhost:5080";

using var channel = GrpcChannel.ForAddress(grpcUrl);

// The generated "stub": it has the same methods as the service, so a remote call
// reads like a local one.
var client = new AgentDeskService.AgentDeskServiceClient(channel);

//Think about error handling here as we are just capturing the args[0] without checking if it exists. This is just a demo, so we will keep it simple.
var command = args[0].ToLowerInvariant();
var parameter = args.Length > 1 ? args[1] : string.Empty;

string Arg(int index, string fallback) => args.Length > index + 1 ? args[index + 1] : fallback;

try
{
    switch (command)
    {
        case "get":
            var ticketId = int.Parse(Arg(1, "1"));
            await UnaryDemo.GetTicketAsync(client, ticketId);
            break;

        case "assign":
            await UnaryDemo.AssignTicketAsync(client, int.Parse(Arg(0, "2")));
            break;

        case "watch":
            await WatchQueueDemo.RunAsync(client);
            break;

        default:
            Console.WriteLine($"Unknown command: {command}");
            break;
    }
}
catch (RpcException ex)
{
    Console.WriteLine($"Code: {ex.Status.StatusCode}, gRPC error: {ex.Status.Detail}");
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}