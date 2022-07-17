package com.xgd.hws;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

/**
 * @author huwansong
 * @version 1.0, 2022/7/18 8:41
 * @since
 */
public class CFTest {
    @Test
    void complete() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        if (cf.complete(null)) {
            System.out.println("complete");
        } else {
            System.out.println("not ");
        }
    }

    @Test
    void completed() throws InterruptedException {
        CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> System.out.println("1+1=2"));
        Thread.sleep(1000);
        if (cf.complete(null)) {
            System.out.println("complete");
        } else {
            System.out.println("not ");
        }
    }

}
