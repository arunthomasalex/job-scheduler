package com.run.framework.core;

import static com.run.framework.job.util.Trampoline.done;
import static com.run.framework.job.util.Trampoline.more;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.run.framework.job.util.Trampoline;

public class TrampolineTest {

	@Test
	public void test_multiply_till_1() {
		assertEquals(multiplyTill(1, 1).get().intValue(), 1);
	}

	@Test
	public void test_multiply_till_2() {
		assertEquals(multiplyTill(2, 1).get().intValue(), 2);
	}

	@Test
	public void test_multiply_till_3() {
		assertEquals(multiplyTill(3, 1).get().intValue(), 6);
	}

	@Test
	public void test_multiply_till_4() {
		assertEquals(multiplyTill(4, 1).get().intValue(), 24);
	}

	@Test
	public void test_multiply_till_5() {
		assertEquals(multiplyTill(5, 1).get().intValue(), 120);
	}

	@Test
	public void test_add_till_1() {
		assertEquals(addTill(1, 0).get().intValue(), 1);
	}

	@Test
	public void test_add_till_2() {
		assertEquals(addTill(2, 0).get().intValue(), 3);
	}

	@Test
	public void test_add_till_3() {
		assertEquals(addTill(3, 0).get().intValue(), 6);
	}

	@Test
	public void test_add_till_4() {
		assertEquals(addTill(4, 0).get().intValue(), 10);
	}

	@Test
	public void test_add_till_5() {
		assertEquals(addTill(5, 0).get().intValue(), 15);
	}

	public Trampoline<Integer> multiplyTill(int times, int prod) {
		if (times == 0) {
			return done(prod);
		} else {
			return more(() -> multiplyTill(times - 1, prod * times));
		}
	}

	public Trampoline<Integer> addTill(int n, int accumulate) {
		if (n == 0) {
			return done(accumulate);
		} else {
			return more(() -> addTill(n - 1, accumulate + n));
		}
	}
}
