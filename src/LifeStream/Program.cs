using System;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.FOperationAPI;
using Streamer.Ingest;
using System.Collections.Generic;
using System.Linq;

namespace LifeStream
{
    class Program
    {
        
        static double NonFuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<IStreamable<Empty, Signal>, IStreamable<Empty, TResult>> transform)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var s_obs = transform(stream);

            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double FuseTest<TResult>(Func<IStreamable<Empty, Signal>> data,
            Func<FOperation<Signal>, FOperation<TResult>> transform)
        {
            var stream = data();

            var sw = new Stopwatch();
            sw.Start();
            var fStart = stream
                    .FuseStart()
                ;

            var s_obs = transform(fStart.GetFOP())
                    .FuseEnd()
                ;

            fStart.Connect();
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        static double MultiNonFuseTest(
            Func<IStreamable<Empty, Signal>> abp_data, Func<IStreamable<Empty, Signal>> ecg_data)
        {
            var abp = abp_data();
            var ecg = ecg_data();

            var sw = new Stopwatch();
            sw.Start();
            var window = 60000;
            var gap_tol = 60000;
            var fstream1 = abp
                    .FillMean(window, 8, gap_tol)
                    .Resample(8, 2)
                    .Normalize(window)
                ;
            var fstream2 = ecg
                    .FillMean(window, 2, gap_tol)
                    .Normalize(window)
                ;

            var s_obs = fstream2
                    .Join(fstream1, (l, r) => new {l, r})
                ;
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        internal static void PairJoiner(Signal l, Signal r, out FStreamable.SigPair o)
        {
            o.s = l;
            o.e = r;
        }
        
        static double MultiFuseTest(Func<IStreamable<Empty, Signal>> abp_data,
            Func<IStreamable<Empty, Signal>> ecg_data)
        {
            var abp = abp_data();
            var ecg = ecg_data();

            var sw = new Stopwatch();
            sw.Start();
            var fabp = abp
                    .FuseStart()
                ;
            var fecg = ecg
                    .FuseStart()
                ;

            var window = 60000;
            var gap_tol = 60000;
            var fstream1 = fabp.GetFOP()
                    .FillMean(window, 8, gap_tol)
                    .Resample(8, 2, 8)
                    .Normalize(2, window)
                ;
            var fstream2 = fecg.GetFOP()
                    .FillMean(window, 2, gap_tol)
                    .Normalize(2, window)
                ;

            var s_obs = fstream2
                    .Join<Signal, Signal, FStreamable.SigPair>(fstream1, PairJoiner)
                    .FuseEnd()
                ;

            fabp.Connect();
            fecg.Connect();
            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            return sw.Elapsed.TotalSeconds;
        }

        public struct TwoStructs
        {
            public TwoStructs(Longs val)
            {
                a = val;
                b = val;
            }
            public Longs a;
            public Longs b;
        }
        
        public struct Longs
        {
            public Longs(long val)
            {
                a = val;
                b = val;
                c = val;
                d = val;
                
                e = val;
                f = val;
                g = val;
                h = val;
                
                i = val;
                j = val;
                k = val;
                l = val;
                
                m = val;
                n = val;
                o = val;
                p = val;
                
                q = val;
                r = val;
                s = val;
                t = val;
                
                u = val;
                v = val;
                w = val;
                x = val;
                
                a1 = val;
                b1 = val;
                c1 = val;
                d1 = val;
                
                e1 = val;
                f1 = val;
                g1 = val;
                h1 = val;
                
                i1 = val;
                j1 = val;
                k1 = val;
                l1 = val;
                
                m1 = val;
                n1 = val;
                o1 = val;
                p1 = val;
                
                q1 = val;
                r1 = val;
                s1 = val;
                t1 = val;
                
                u1 = val;
                v1 = val;
                w1 = val;
                x1 = val;
                
                a2 = val;
                b2 = val;
                c2 = val;
                d2 = val;
                
                e2 = val;
                f2 = val;
                g2 = val;
                h2 = val;
                
                i2 = val;
                j2 = val;
                k2 = val;
                l2 = val;
                
                m2 = val;
                n2 = val;
                o2 = val;
                p2 = val;

            }
            public long a;
            public long b;
            public long c;
            public long d;

            public long e;
            public long f;
            public long g;
            public long h;
            
            public long i;
            public long j;
            public long k;
            public long l;

            public long m;
            public long n;
            public long o;
            public long p;
            
            public long q;
            public long r;
            public long s;
            public long t;

            public long u;
            public long v;
            public long w;
            public long x;
            
            public long a1;
            public long b1;
            public long c1;
            public long d1;
            
            public long e1;
            public long f1;
            public long g1;
            public long h1;
            
            public long i1;
            public long j1;
            public long k1;
            public long l1;
            
            public long m1;
            public long n1;
            public long o1;
            public long p1;
            
            public long q1;
            public long r1;
            public long s1;
            public long t1;

            public long u1;
            public long v1;
            public long w1;
            public long x1;
            
            public long a2;
            public long b2;
            public long c2;
            public long d2;
            
            public long e2;
            public long f2;
            public long g2;
            public long h2;
            
            public long i2;
            public long j2;
            public long k2;
            public long l2;
            
            public long m2;
            public long n2;
            public long o2;
            public long p2;

        }

        static void ChangingTypes(string test)
        {
            Config.DataBatchSize = 120000;
            Config.FuseFactor = 1;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.ForceRowBasedExecution = true;
            StreamCache<Empty, int> stream;
            /*
            switch (test)
            {
                case "char":
                    var listA = new List<char>();
                    for (int j = 0; j < 500; j++)
                    {
                        for (int i = 0; i < 60000; i++)
                        {
                            listA.Add((char) i);
                        }
                    }

                    var data = new {ts = 0, p = listA[0]};
                    var list = new[] {data}.ToList();
                    for (int i = 1; i < listA.Count; i++)
                    {
                        data = new {ts = i, p = listA[i]};
                        list.Add(data);
                    }
                    //StreamCache<Empty, int> streamA;
                    var streamA = list
                        .ToObservable()
                        .ToTemporalStreamable(e => e.ts, e => e.ts + 1)
                        .Select(e => e.p)
                        .Cache();
                    break;
                case "int":
                    var listB = new List<int>();
                    for (int i = 0; i < 30000000; i++)
                    {
                        listB.Add(i);
                    }

                    var streamB = listB
                        .ToObservable()
                        .ToTemporalStreamable(e => e, e => e + 1)
                        .Cache();
                    break;
                case "long":
                    var listC = new List<long>();
                    for (int i = 0; i < 30000000; i++)
                    {
                        listC.Add(i);
                    }

                    var streamC = listC
                            .ToObservable()
                            .ToTemporalStreamable(e => e, e => e + 1)
                            .Cache()
                        ;
                    break;
                case "longs":
                    var listD = new List<Longs>();
                    for (int i = 0; i < 30000000; i++)
                    {
                        Longs p = new Longs(i);

                        listD.Add(p);
                    }

                    var streamD = listD
                            .ToObservable()
                            .ToTemporalStreamable(e => e.a, e => (e.a + 1))
                            .Cache()
                        ;
                    break;
                default:
                    Console.Error.WriteLine("no");
                    return;
            }
            */
            var listD = new List<TwoStructs>();
            for (int i = 0; i < 30000000; i++)
            {
                Longs p = new Longs(i);
                TwoStructs q = new TwoStructs(p);
                listD.Add(q);
            }

            var streamA = listD
                    .ToObservable()
                    .ToTemporalStreamable(e => e.a.b, e => (e.a.b + 1))
                    .Cache()
                ;
            //ops
            
            var sw = new Stopwatch();
            sw.Start();
            var s_obs = streamA
                .Select(e => e.a.b + 1);

            s_obs
                .ToStreamEventObservable()
                .Wait();
            sw.Stop();
            
            Console.WriteLine("Op:Select,  Time: {0:.###} sec",
                sw.Elapsed.TotalSeconds);
            
            var sw2 = new Stopwatch();
            sw2.Start();
            var s_obs2 = streamA
                .Where(e => e.a.b == 0);

            s_obs2
                .ToStreamEventObservable()
                .Wait();
            sw2.Stop();
            
            Console.WriteLine("Op:Where,  Time: {0:.###} sec",
                sw2.Elapsed.TotalSeconds);
            
            var sw3 = new Stopwatch();
            sw3.Start();
            var s_obs3 = streamA
                .Join(streamA, (l,r)=> l);

            s_obs3
                .ToStreamEventObservable()
                .Wait();
            sw3.Stop();
            
            Console.WriteLine("Op:Join,  Time: {0:.###} sec",
                sw3.Elapsed.TotalSeconds);
            
            var sw4 = new Stopwatch();
            sw4.Start();
            var s_obs4 = streamA
                .TumblingWindowLifetime(100000)
                .Aggregate(w=> w.Count());

            s_obs4
                .ToStreamEventObservable()
                .Wait();
            sw4.Stop();
            Console.WriteLine("Op:Aggregate,  Time: {0:.###} sec",
                sw4.Elapsed.TotalSeconds);
            
            Config.StreamScheduler.Stop();
        }

        static void BenchMarks(int dur, string eng , string test )
        {
            Config.DataBatchSize = 120000;
            Config.FuseFactor = 1;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.ForceRowBasedExecution = true;

            int duration = dur;
            var testcase = test; //normalize, passfilter, fillconst, fillmean, resample, endtoend
            var engine = eng;
            double time = 0;

            const int start = 0;
            const int freq = 500;
            const int period = 1000 / freq;
            const long window = 60000;
            const long gap_tol = window;
            long count = (duration * freq);
            Config.DataGranularity = window;

            
            
            Func<IStreamable<Empty, Signal>> data = () =>
            {
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                        .Cache()
                    ;
            };

            Func<IStreamable<Empty, Signal>> abp_data = () =>
            {
                const int freq = 125;
                const int period = 1000 / freq;
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };

            Func<IStreamable<Empty, Signal>> ecg_data = () =>
            {
                const int freq = 500;
                const int period = 1000 / freq;
                return new TestObs("test", start, duration, freq)
                        .Select(e => e.Payload)
                        .ToTemporalStreamable(e => e.ts, e => e.ts + period)
                    ;
            };
            
            
            switch (testcase + "_" + engine)
            {
                case "normalize_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .Normalize(window)
                    );
                    break;
                case "normalize_lifestream":
                    time = FuseTest(data, stream =>
                            stream
                                .Normalize(period, window))
                        ;
                    break;
                case "passfilter_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .BandPassFilter(period, window, 2, 200)
                    );
                    break;
                case "passfilter_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .BandPassFilter(period, window, 2, 200)
                    );
                    break;
                case "fillconst_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .FillConst(period, gap_tol, 0)
                    );
                    break;
                case "fillconst_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .FillConst(period, gap_tol, 0)
                    );
                    break;
                case "fillmean_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .FillMean(window, period, gap_tol)
                    );
                    break;
                case "fillmean_lifestream":
                    time = FuseTest(data, stream =>
                        stream
                            .FillMean(window, period, gap_tol)
                    );
                    break;
                case "resample_trill":
                    time = NonFuseTest(data, stream =>
                        stream
                            .Resample(period, period / 2)
                    );
                    break;
                case "resample_lifestream":
                    Config.FuseFactor = (int) (window / period);
                    time = FuseTest(data, stream =>
                        stream
                            .Resample(period, period / 2, period)
                    );
                    break;
                case "endtoend_trill":
                    count = duration * (500 + 125);
                    time = MultiNonFuseTest(abp_data, ecg_data);
                    break;
                case "endtoend_lifestream":
                    count = duration * (500 + 125);
                    time = MultiFuseTest(abp_data, ecg_data);
                    break;
                default:
                    Console.Error.WriteLine("Unknown benchmark combination {0} on {1}", testcase, engine);
                    return;
            }
            Config.StreamScheduler.Stop();
        }
        public class Payload
        {
            private byte[] data;

            public Payload (int size, byte value)
            {
                data = new byte[size];
                for (int i = 0; i < size; i++)
                {
                    data[i] = value;
                }
            }
            
        }

        public void Aggregate_Bench(stream)
        {
            var result = 
        }
        public void Bench(int data_size, int payload_size)
        {
            
        }
        
        static void Main(string[] args)
        {
            int data_size = 30000000;
            int payload_size = 1;
            Bench(data_size, payload_size);
        }
    }
}
