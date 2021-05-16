using System;

namespace Streamer.Ingest
{
    public struct Signal : IComparable<Signal>
    {
        public long ts;
        public float val;
        public float val2;
        public float val3;
        public float val4;
        public float val5;

        public Signal(long ts, float val, float val2, float val3, float val4, float val5)
        {
            this.ts = ts;
            this.val = val;
            this.val2 = val2;
            this.val3 = val3;
            this.val4 = val4;
            this.val5 = val5;
        }

        public int CompareTo(Signal other)
        {
            return (int) (this.ts - other.ts);
        }

        public override string ToString()
        {
            return $"ts={this.ts}, val={this.val}";
        }
    }
}