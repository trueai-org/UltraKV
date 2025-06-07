namespace UltraKV;

/// <summary>
/// 索引预留信息
/// </summary>
public class IndexReservation
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

    public bool IsValid => !string.IsNullOrEmpty(Key) && DataPosition >= 0;

    public override string ToString()
    {
        return $"IndexReservation: Key={Key}, DataPos={DataPosition}, PageIndex={PageIndex}";
    }
}