using SupportHub.AgentDesk.V1;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace AgentDesk.Server.Services;

/// <summary>
/// One chat room per ticket. A singleton, so two console clients running in two terminals end up in the same room.
/// </summary>
public sealed class ChatRoomRegistry
{
    private readonly ConcurrentDictionary<int, ChatRoom> _rooms = new();

    public ChatRoom GetOrCreate(int ticketId) => _rooms.GetOrAdd(ticketId, _ => new ChatRoom());
}

// <summary>
/// Everybody currently on one ticket's chat. Exactly the same fan-out idea as the watchers in TicketStore: 
/// one channel per participant, and a broadcast writes to all of them. Each Chat call owns one Membership and reads from its own inbox.
/// </summary>
public sealed class ChatRoom
{
    private readonly ConcurrentDictionary<Guid, Channel<ChatMessage>> _members = new();

    public int Count => _members.Count;

    public Membership Join()
    {
        var channel = Channel.CreateUnbounded<ChatMessage>(new UnboundedChannelOptions { SingleReader = true });

        var id = Guid.NewGuid();

        _members[id] = channel;

        return new Membership(channel.Reader, () =>
        {
            if (_members.TryRemove(id, out var gone)) gone.Writer.TryComplete();
        });
    }

    /// <summary>Puts one message in everybody's inbox — including the sender's.</summary>
    public void Broadcast(ChatMessage message)
    {
        foreach (var member in _members.Values)
            member.Writer.TryWrite(message);
    }

    public sealed class Membership(ChannelReader<ChatMessage> inbox, Action onDispose) : IDisposable
    {
        public ChannelReader<ChatMessage> Inbox { get; } = inbox;

        public void Dispose() => onDispose();
    }
}