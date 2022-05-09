package com.run.framework.job.util;

import java.util.function.Supplier;

public interface Trampoline<T> {
	T get();

	static <T> Trampoline<T> done(final T t) {
		return () -> t;
	}

	static <T> Trampoline<T> more(Supplier<Trampoline<T>> tramp) {
		return new Trampoline<T>() {
			@Override
			public T get() {
				return trampoline(tramp.get());
			}

			// Used to mask what the return type of tramp.get() function.
			private T trampoline(final Trampoline<T> t) {
				return t.get();
			}

		};
	}

	default T execute(Trampoline<T> tramp) {
		return tramp.get();
	}
}
