package io.netty.disk;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * User: liuxiong
 * Date: 13-9-23
 * Time: 上午9:51
 */
public class SingleLinkedListTest {

    @Test
    public void testSingleLinkedList() {
        final int SIZE = 10;
        SingleLinkedList<Integer> intList = new SingleLinkedList<Integer>();

        for (int i = 0; i < SIZE; i++) {
            intList.addLast(i);
        }

        assertEquals(SIZE, intList.size());

        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, (int)intList.takeFirst());
        }

        assertEquals(0, intList.size());
        assertEquals(null, intList.takeFirst());
    }

}
