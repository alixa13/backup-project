package io.netsecml.platform.domain.event;

public sealed interface MappingResult<T> permits MappingResult.Valid, MappingResult.Invalid {

    record Valid<T>(T value) implements MappingResult<T> {
        public Valid {
            if (value == null) {
                throw new IllegalArgumentException("valid() requires a non-null value");
            }
        }
    }

    record Invalid<T>(ReasonCode reason, String detail) implements MappingResult<T> {
        public Invalid {
            if (reason == null) {
                throw new IllegalArgumentException("invalid() requires a non-null reason");
            }
        }
    }

    static <T> MappingResult<T> valid(T value) {
        return new Valid<>(value);
    }

    static <T> MappingResult<T> invalid(ReasonCode reason, String detail) {
        return new Invalid<>(reason, detail);
    }

    default boolean isValid() {
        return switch (this) {
            case Valid<T> v -> true;
            case Invalid<T> i -> false;
        };
    }

    default T value() {
        return switch (this) {
            case Valid<T> v -> v.value();
            case Invalid<T> i -> throw new IllegalStateException("MappingResult is invalid, reason=" + i.reason());
        };
    }

    default ReasonCode reason() {
        return switch (this) {
            case Invalid<T> i -> i.reason();
            case Valid<T> v -> throw new IllegalStateException("MappingResult is valid, no reason available");
        };
    }

    default String detail() {
        return switch (this) {
            case Invalid<T> i -> i.detail();
            case Valid<T> v -> throw new IllegalStateException("MappingResult is valid, no detail available");
        };
    }
}
