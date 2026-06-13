using System.Net;
using System.Text.Json;
using Infrastructure.SmbSyncService;
using Microsoft.Extensions.Options;

namespace Services.SmbSyncService;

public class SyncWorker : BackgroundService
{
    private readonly ILogger<SyncWorker> _logger;
    private readonly IOptionsMonitor<SyncConfig> _configMonitor;

    public SyncWorker(ILogger<SyncWorker> logger, IOptionsMonitor<SyncConfig> configMonitor)
    {
        _logger = logger;
        _configMonitor = configMonitor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var config = _configMonitor.CurrentValue;

            TimeSpan delay = CalculateDelayUntilNextRun(config.SyncTime);
            
            _logger.LogInformation("Next sync scheduled in {DelayHours} hours and {DelayMinutes} minutes.", 
                delay.Hours, delay.Minutes);

            try
            {
                await Task.Delay(delay, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break; 
            }

            if (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Executing scheduled synchronization for {Count} pairs...", config.Pairs.Count);
                
                // Loop through each folder pair in the config
                foreach (var pair in config.Pairs)
                {
                    try
                    {
                        ProcessPair(pair);
                    }
                    catch (Exception ex)
                    {
                        // Catch errors per-pair so a failure in one doesn't crash the others
                        _logger.LogError(ex, "[{JobId}] An error occurred during synchronization.", pair.JobId);
                    }
                }
            }
        }
    }

    private void ProcessPair(SyncPair pair)
    {
        if (string.IsNullOrWhiteSpace(pair.JobId))
        {
            _logger.LogWarning("Skipping a sync pair because it has no JobId defined.");
            return;
        }

        _logger.LogInformation("[{JobId}] Starting sync...", pair.JobId);
        string stateFilePath = Path.Combine(AppContext.BaseDirectory, $"syncstate_{pair.JobId}.json");

        var credentials = new NetworkCredential(pair.Username, pair.Password);

        using (new NetworkConnection(pair.SmbPath, credentials))
        {
            EnsureDirectoryExists(pair.LocalPath);
            EnsureDirectoryExists(pair.SmbPath);

            var previousState = LoadState(stateFilePath);
            var currentState = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

            var localFiles = GetFilesWithTimestamps(pair.LocalPath);
            var smbFiles = GetFilesWithTimestamps(pair.SmbPath);

            // 1. Process Deletions
            ProcessDeletions(previousState, localFiles, smbFiles, pair.LocalPath, pair.SmbPath, pair.JobId);

            // 2. Process Additions and Modifications
            var allRemainingFiles = localFiles.Keys.Union(smbFiles.Keys).Distinct(StringComparer.OrdinalIgnoreCase);

            foreach (var relativePath in allRemainingFiles)
            {
                var inLocal = localFiles.TryGetValue(relativePath, out var localTime);
                var inSmb = smbFiles.TryGetValue(relativePath, out var smbTime);

                var localFullPath = Path.Combine(pair.LocalPath, relativePath);
                var smbFullPath = Path.Combine(pair.SmbPath, relativePath);

                if (inLocal && !inSmb)
                {
                    CopyFile(localFullPath, smbFullPath, pair.JobId);
                    currentState[relativePath] = File.GetLastWriteTimeUtc(smbFullPath);
                }
                else if (!inLocal && inSmb)
                {
                    CopyFile(smbFullPath, localFullPath, pair.JobId);
                    currentState[relativePath] = File.GetLastWriteTimeUtc(localFullPath);
                }
                else if (inLocal && inSmb)
                {
                    if (localTime > smbTime.AddSeconds(1))
                    {
                        CopyFile(localFullPath, smbFullPath, pair.JobId);
                        currentState[relativePath] = File.GetLastWriteTimeUtc(smbFullPath);
                    }
                    else if (smbTime > localTime.AddSeconds(1))
                    {
                        CopyFile(smbFullPath, localFullPath, pair.JobId);
                        currentState[relativePath] = File.GetLastWriteTimeUtc(localFullPath);
                    }
                    else
                    {
                        currentState[relativePath] = localTime;
                    }
                }
            }

            SaveState(stateFilePath, currentState, pair.JobId);
            _logger.LogInformation("[{JobId}] Complete.", pair.JobId);
        }
    }

    private void ProcessDeletions(
        Dictionary<string, DateTime> previousState, 
        Dictionary<string, DateTime> localFiles, 
        Dictionary<string, DateTime> smbFiles,
        string localRoot,
        string smbRoot,
        string jobId)
    {
        foreach (var fileInState in previousState.Keys)
        {
            bool deletedLocally = !localFiles.ContainsKey(fileInState);
            bool deletedOnSmb = !smbFiles.ContainsKey(fileInState);

            if (deletedLocally && !deletedOnSmb)
            {
                var targetPath = Path.Combine(smbRoot, fileInState);
                try
                {
                    File.Delete(targetPath);
                    smbFiles.Remove(fileInState);
                    _logger.LogInformation("[{JobId}] Propagated deletion to SMB: {File}", jobId, fileInState);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[{JobId}] Failed to delete file on SMB: {File}", jobId, targetPath);
                }
            }
            else if (deletedOnSmb && !deletedLocally)
            {
                var targetPath = Path.Combine(localRoot, fileInState);
                try
                {
                    File.Delete(targetPath);
                    localFiles.Remove(fileInState);
                    _logger.LogInformation("[{JobId}] Propagated deletion to Local: {File}", jobId, fileInState);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[{JobId}] Failed to delete file locally: {File}", jobId, targetPath);
                }
            }
        }
    }

    private void CopyFile(string source, string destination, string jobId)
    {
        try
        {
            var destDir = Path.GetDirectoryName(destination);
            if (destDir != null && !Directory.Exists(destDir))
            {
                Directory.CreateDirectory(destDir);
            }

            File.Copy(source, destination, overwrite: true);
            _logger.LogInformation("[{JobId}] Copied: {Source} -> {Destination}", jobId, source, destination);
        }
        catch (IOException ex)
        {
            _logger.LogWarning("[{JobId}] File locked or inaccessible: {Msg}", jobId, ex.Message);
        }
    }

    private Dictionary<string, DateTime> GetFilesWithTimestamps(string rootPath)
    {
        var dict = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        foreach (var file in Directory.GetFiles(rootPath, "*.*", SearchOption.AllDirectories))
        {
            var relativePath = Path.GetRelativePath(rootPath, file);
            dict[relativePath] = File.GetLastWriteTimeUtc(file);
        }
        return dict;
    }

    private Dictionary<string, DateTime> LoadState(string stateFilePath)
    {
        if (!File.Exists(stateFilePath))
            return new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        try
        {
            var json = File.ReadAllText(stateFilePath);
            return JsonSerializer.Deserialize<Dictionary<string, DateTime>>(json) 
                   ?? new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        }
        catch
        {
            return new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        }
    }

    private void SaveState(string stateFilePath, Dictionary<string, DateTime> state, string jobId)
    {
        try
        {
            var json = JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(stateFilePath, json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[{JobId}] Failed to save sync state.", jobId);
        }
    }

    private void EnsureDirectoryExists(string path)
    {
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }
    }

    private TimeSpan CalculateDelayUntilNextRun(TimeSpan targetTime)
    {
        DateTime now = DateTime.Now;
        DateTime nextRun = now.Date.Add(targetTime);

        if (now > nextRun)
        {
            nextRun = nextRun.AddDays(1);
        }

        return nextRun - now;
    }
}