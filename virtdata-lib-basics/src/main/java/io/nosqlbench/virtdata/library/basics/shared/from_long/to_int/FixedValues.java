package io.nosqlbench.virtdata.library.basics.shared.from_long.to_int;

import io.nosqlbench.virtdata.api.annotations.ThreadSafeMapper;

import java.util.function.LongToIntFunction;

@ThreadSafeMapper
public class FixedValues implements LongToIntFunction {

    private final int[] fixedValues;

    public FixedValues(int... values) {
        this.fixedValues = values;
    }

    @Override
    public int applyAsInt(long value) {
        int index = (int)(value % Integer.MAX_VALUE) % fixedValues.length;
        return fixedValues[index];
    }
}
