namespace LoadSimulator.Shared
{
    public struct DataPayload
    {
        public Guid Id { get; set; }
        public string Content { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
