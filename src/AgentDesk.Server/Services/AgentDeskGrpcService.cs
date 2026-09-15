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

    private readonly TimeProvider _clock = TimeProvider.System;
    public AgentDeskGrpcService(TicketStore ticketStore, ILogger<AgentDeskGrpcService> logger, TimeProvider clock)
    {
        _store = ticketStore;
        _logger = logger;
        _clock = clock;
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
        }

        return queueEvent;
    }

    private Timestamp Now() => Timestamp.FromDateTimeOffset(_clock.GetUtcNow());
}
