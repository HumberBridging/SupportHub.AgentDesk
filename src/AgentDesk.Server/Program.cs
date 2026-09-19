using AgentDesk.Server.Services;
using SupportHub.AgentDesk.Server.Domain;
using SupportHub.AgentDesk.Server.Services;

var builder = WebApplication.CreateBuilder(args);

//Services
builder.Services.AddGrpc(options => 
{
    // Send exception details back to the caller in Development only.
    // In production a stack trace in a status message is a gift to an attacker.
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<TicketStore>();

//Bi-directional
builder.Services.AddSingleton<ChatRoomRegistry>();

var app = builder.Build();

app.MapGrpcService<AgentDeskGrpcService>();

app.MapGet("/", () => "SupportHub AgentDesk speaks gRPC on this port. Use the console client.");

app.Run();

