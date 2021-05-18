using System;
using System.Windows.Markup;

namespace Streamer.Ingest
{
    public struct Signal : IComparable<Signal>
    {
        public long ts;
        public float val;
        public float[] vals;
        

        public Signal(long ts, float val)
        {
            this.ts = ts;
            this.val = val;
            
            int size = 5;
            float[] values = new float [size];
            for (int i =0; i<size; i++)
            {
                values[i] = val;
            }
            this.vals = values;
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