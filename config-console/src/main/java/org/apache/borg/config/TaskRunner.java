package org.apache.borg.config;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.concurrent.ThreadFactory;

@Slf4j
public class TaskRunner {

    private ThreadFactory threadFactory;
    private EventFactory<Element> factory;
    private EventHandler<Element> handler;
    private final BlockingWaitStrategy strategy = new BlockingWaitStrategy();
    private Disruptor<Element> disruptor;
    private RingBuffer<Element> ringBuffer;

    public TaskRunner(){
    }

    public TaskRunner(EventHandler<Element> handler){
        this.handler = handler;
    }

    public static TaskRunner of(EventHandler<Element> handler){
        return new TaskRunner(handler);
    }

    public static TaskRunner create(){
        return new TaskRunner();
    }

    public void threadFactory(String name){
        this.threadFactory = r -> new Thread(r, name);
    }

    public void eventFactory(){
        this.factory = Element::new;
    }

    public TaskRunner handler(EventHandler<Element> handler){
        this.handler = handler;
        return this;
    }

    public TaskRunner disruptor(String name){
        threadFactory(name);
        eventFactory();
        // 创建disruptor，采用单生产者模式
        int bufferSize = 8;
        disruptor = new Disruptor(this.factory, bufferSize, this.threadFactory, ProducerType.SINGLE, strategy);
        // 设置EventHandler
        disruptor.handleEventsWith(this.handler);
        return this;
    }

    public TaskRunner start(){
        // 启动disruptor的线程
        disruptor.start();
        return this;
    }

    public TaskRunner stop(){
        // 启动disruptor的线程
        disruptor.shutdown();
        return this;
    }

    public void publish(PartnerSignInsurance signInsurance, Boolean isDriver,Integer model){
        if(ringBuffer == null){
            ringBuffer = disruptor.getRingBuffer();
            log.info("franchisee-publish get ringBuffer:{}",
                    ringBuffer.getBufferSize());
        }
        // 获取下一个可用位置的下标
        long sequence = ringBuffer.next();
        try{
            // 返回可用位置的元素
            Element event = ringBuffer.get(sequence);
            // 设置该位置元素的值

            event.set(new ImmutableTriple<>(signInsurance, isDriver,model));
            log.info("franchisee-publish franchiseeId:{}, isDriver:{}",
                    signInsurance.getFranchiseeId(),isDriver);
        }catch (Exception e){
            log.error("franchisee-send error",e);
        }finally{
            ringBuffer.publish(sequence);
        }
    }
}
