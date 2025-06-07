namespace UltraKV;

/// <summary>
/// 索引预留信息
/// </summary>
public struct IndexReservation
{
    public string Key;
    public long DataPosition;
    public IndexEntry IndexEntry;
    public uint PageIndex;
    public DateTime ReservationTime;

    public IndexReservation(string key, long dataPosition, IndexEntry indexEntry, uint pageIndex)
    {
        Key = key;
        DataPosition = dataPosition;
        IndexEntry = indexEntry;
        PageIndex = pageIndex;
        ReservationTime = DateTime.UtcNow;
    }

    public readonly bool IsValid => !string.IsNullOrEmpty(Key) && DataPosition >= 0;

    public override readonly string ToString()
    {
        return $"IndexReservation: Key={Key}, DataPos={DataPosition}, PageIndex={PageIndex}";
    }
}