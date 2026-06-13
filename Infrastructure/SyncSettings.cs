namespace Infrastructure.SmbSyncService;

public class SyncConfig
{
    public TimeSpan SyncTime { get; set; } = new TimeSpan(2, 0, 0);
    public List<SyncPair> Pairs { get; set; } = new();
}

public class SyncPair
{
    public string JobId { get; set; } = string.Empty;
    public string LocalPath { get; set; } = string.Empty;
    public string SmbPath { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}