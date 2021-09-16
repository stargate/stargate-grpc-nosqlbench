package io.nosqlbench.grpc.binders;

import com.google.protobuf.Any;
import io.nosqlbench.virtdata.core.bindings.ValuesArrayBinder;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ValuesBinder implements ValuesArrayBinder<Object, Payload> {

    public abstract static class Codec<T> {

        private final Class<T> javaType;

        public Codec(Class<T> javaType) {
            this.javaType = javaType;
        }

        public boolean accepts(Object value) {
            return javaType.isAssignableFrom(value.getClass());
        }

        public abstract Value encode(Object value);
    }

    public static final Codec<?>[] CODECS = new Codec[]{
        BooleanCodec.instance,
        BigintCodec.instance,
        IntCodec.instance,
        SmallintCodec.instance,
        TinyintCodec.instance,
        DoubleCodec.instance,
        FloatCodec.instance,
        StringCodec.instance,
        UUIDCodec.instance,
        DateCodec.instance,
        TimestampCodec.instance,
        CollectionCodec.instance,
        MapCodec.instance
    };

    public static Value fromObject(Object value) {
        for (Codec<?> codec : CODECS) {
            if (codec.accepts(value)) {
                return codec.encode(value);
            }
        }
        throw new UnsupportedOperationException(
            "Unable to bind unhandled value of type " + value.getClass());
    }

    @Override
    public Payload bindValues(Object context, Object[] values) {
        if (values.length > 0) {
            Values.Builder valuesBuilder = Values.newBuilder();

            for (Object value : values) {
                valuesBuilder.addValues(fromObject(value));
            }

            return Payload.newBuilder().setData(Any.pack(valuesBuilder.build())).build();
        } else {
            return null;
        }
    }

    public static class BooleanCodec extends Codec<Boolean> {

        public static final BooleanCodec instance = new BooleanCodec();

        public BooleanCodec() {
            super(Boolean.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((long) value);
        }
    }

    public static class BigintCodec extends Codec<Long> {

        public static final BigintCodec instance = new BigintCodec();

        public BigintCodec() {
            super(Long.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((long) value);
        }
    }

    public static class IntCodec extends Codec<Integer> {

        public static final IntCodec instance = new IntCodec();

        public IntCodec() {
            super(Integer.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((long) value);
        }
    }

    public static class SmallintCodec extends Codec<Short> {

        public static final SmallintCodec instance = new SmallintCodec();

        public SmallintCodec() {
            super(Short.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((short) value);
        }
    }

    public static class TinyintCodec extends Codec<Byte> {

        public static final TinyintCodec instance = new TinyintCodec();

        public TinyintCodec() {
            super(Byte.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((byte) value);
        }
    }

    public static class FloatCodec extends Codec<Float> {

        public static final FloatCodec instance = new FloatCodec();

        public FloatCodec() {
            super(Float.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((float) value);
        }
    }

    public static class DoubleCodec extends Codec<Double> {

        public static final DoubleCodec instance = new DoubleCodec();

        public DoubleCodec() {
            super(Double.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((double) value);
        }
    }

    public static class StringCodec extends Codec<String> {

        public static final StringCodec instance = new StringCodec();

        public StringCodec() {
            super(String.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((String) value);
        }
    }

    public static class UUIDCodec extends Codec<UUID> {

        public static final UUIDCodec instance = new UUIDCodec();

        public UUIDCodec() {
            super(UUID.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((UUID) value);
        }
    }

    public static class DateCodec extends Codec<LocalDate> {

        public static final DateCodec instance = new DateCodec();

        public DateCodec() {
            super(LocalDate.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of((LocalDate) value);
        }
    }

    public static class TimestampCodec extends Codec<Date> {

        public static final TimestampCodec instance = new TimestampCodec();

        public TimestampCodec() {
            super(Date.class);
        }

        @Override
        public Value encode(Object value) {
            return io.stargate.grpc.Values.of(((Date)value).getTime());
        }
    }

    @SuppressWarnings("rawtypes")
    public static class CollectionCodec extends Codec<Collection>{

        public static final CollectionCodec instance = new CollectionCodec();

        public CollectionCodec() {
            super(Collection.class);
        }

        @Override
        public Value encode(Object value) {

            Collection<?> values = (Collection<?>) value;
            QueryOuterClass.Collection.Builder builder = QueryOuterClass.Collection.newBuilder();
            for (Object o : values) {
                builder.addElements(fromObject(o));
            }

            return Value.newBuilder().setCollection(builder).build();
        }
    }

    @SuppressWarnings("rawtypes")
    public static class MapCodec extends Codec<Map>{

        public static final MapCodec instance = new MapCodec();

        public MapCodec() {
            super(Map.class);
        }

        @Override
        public Value encode(Object value) {
            Map<?, ?> map = (Map<?, ?>) value;

            // map is stored as a list of altering k,v
            // For example, for a Map([1,"a"], [2,"b"]) it becomes a List(1,"a",2,"b")
            List<Value> values =
                map.entrySet().stream()
                    .flatMap(v -> Stream.of(v.getKey(), v.getValue()))
                    .map(ValuesBinder::fromObject)
                    .collect(Collectors.toList());

            return Value.newBuilder()
                .setCollection(QueryOuterClass.Collection.newBuilder().addAllElements(values).build())
                .build();
        }
    }

}
