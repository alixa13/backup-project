package io.netsecml.platform.domain.feature;

// Python's collections.deque(maxlen=n) over longs -- the S7 connection state's
// fixed histories -- with the three statistics upstream's builders take over
// one (two-models-info/S7___/customer_icsnpp_enriched_builder.py: _ratio_true,
// _unique_ratio, _normalized_entropy). Appending to a full ring drops the
// oldest entry, exactly as deque.append does at maxlen.
final class LongRing {

    private final long[] values;

    // Index of the oldest entry, and how many entries are held.
    private int head;
    private int size;

    LongRing(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1, was " + capacity);
        }
        this.values = new long[capacity];
    }

    // deque.append: add at the newest end; when full, overwrite the oldest.
    void add(long value) {
        if (size < values.length) {
            values[(head + size) % values.length] = value;
            size++;
        } else {
            values[head] = value;
            head = (head + 1) % values.length;
        }
    }

    int size() {
        return size;
    }

    // The i-th entry, oldest first.
    long get(int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException("index " + i + ", size " + size);
        }
        return values[(head + i) % values.length];
    }

    // sum / len, or 0.0 when empty: _ratio_true over 0/1 flags, the V4 wrapper's
    // sum(direction_history) / len(...), and fmean over integer counts -- fsum
    // of integers is their exact sum, so one division gives the same double.
    double meanOrZero() {
        if (size == 0) {
            return 0.0;
        }
        long sum = 0;
        for (int i = 0; i < size; i++) {
            sum += get(i);
        }
        return (double) sum / size;
    }

    // _unique_ratio: len(set(values)) / len(values), or 0.0 when empty.
    double distinctRatioOrZero() {
        if (size == 0) {
            return 0.0;
        }
        int distinct = 0;
        for (int i = 0; i < size; i++) {
            if (firstIndexOf(get(i)) == i) {
                distinct++;
            }
        }
        return (double) distinct / size;
    }

    // _normalized_entropy(values, maxItems): 0.0 for <= 1 value; otherwise
    // -sum(p * log2(p)) over the distinct values in first-occurrence order (a
    // Python Counter's order), divided by log2(max(2, min(maxItems, n))). A
    // window holding one distinct value gives -0.0, exactly as upstream's
    // negated sum of a single 0.0 term does; keep the negation as written. Plain
    // summation and ln(x)/ln(2) give the same float32 as Python for every input
    // these features can reach (docs/superpowers/specs/2026-09-24-s7comm-stage1-design.md,
    // ruling R7).
    double normalizedEntropy(int maxItems) {
        int n = size;
        if (n <= 1) {
            return 0.0;
        }
        // Count each distinct value, in the order it first appears.
        long[] distinctValues = new long[n];
        int[] counts = new int[n];
        int distinct = 0;
        for (int i = 0; i < n; i++) {
            long value = get(i);
            int j = 0;
            while (j < distinct && distinctValues[j] != value) {
                j++;
            }
            if (j == distinct) {
                distinctValues[distinct] = value;
                counts[distinct] = 1;
                distinct++;
            } else {
                counts[j]++;
            }
        }
        // Shannon entropy in bits, then the upstream normalizer.
        double sum = 0.0;
        for (int j = 0; j < distinct; j++) {
            double p = (double) counts[j] / n;
            sum += p * log2(p);
        }
        double entropy = -sum;
        double denominator = log2(Math.max(2, Math.min(maxItems, n)));
        return denominator > 0 ? entropy / denominator : 0.0;
    }

    // The index of a value's first occurrence, oldest first.
    private int firstIndexOf(long value) {
        for (int i = 0; i < size; i++) {
            if (get(i) == value) {
                return i;
            }
        }
        return -1;
    }

    private static double log2(double x) {
        return Math.log(x) / Math.log(2.0);
    }
}
