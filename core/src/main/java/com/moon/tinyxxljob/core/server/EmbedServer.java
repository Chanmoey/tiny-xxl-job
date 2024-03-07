package com.moon.tinyxxljob.core.server;

import com.moon.tinyxxljob.core.biz.ExecutorBiz;
import com.moon.tinyxxljob.core.biz.client.ExecutorBizClient;
import com.moon.tinyxxljob.core.biz.model.ReturnT;
import com.moon.tinyxxljob.core.biz.model.TriggerParam;
import com.moon.tinyxxljob.core.biz.util.GsonTool;
import com.moon.tinyxxljob.core.biz.util.ThrowableUtil;
import com.moon.tinyxxljob.core.biz.util.XxlJobRemotingUtil;
import com.moon.tinyxxljob.core.thread.ExecutorRegistryThread;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.sctp.nio.NioSctpServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @author Chanmoey
 * Create at 2024-03-06
 */
public class EmbedServer {

    private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);

    private ExecutorBiz executorBiz;

    /**
     * 启动Netty服务器的线程，说明该内嵌服务器的启动是一部的
     */
    private Thread thread;

    public void start(final String address, final int port, final String appname, final String accessToken) {
        // 负责执行定时任务
        executorBiz = new ExecutorBizClient();

        // 这个线程负责启动Netty服务器
        thread = new Thread(() -> {
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();

            // 执行定时任务的线程池，用来和NettyIO之间进行解耦
            ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(
                    0,
                    200,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(2000),
                    r -> new Thread(r, "xxl-job, EmbedServer bizThreadPool-" + r.hashCode()),
                    (r, executor) -> {
                        throw new RuntimeException("xxl-job, EmbedServer bizThreadPool is EXHAUSTED!");
                    });

            try {
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(NioSctpServerChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel channel) throws Exception {
                                channel.pipeline()
                                        //心跳检测
                                        .addLast(new IdleStateHandler(0, 0, 30 * 3, TimeUnit.SECONDS))
                                        //http的编解码器，该处理器既是出站处理器，也是入站处理器
                                        .addLast(new HttpServerCodec())
                                        //这个处理器从名字上就能看出来，是聚合消息的，当传递的http消息过大时，会被拆分开，这里添加这个处理器
                                        //就是把拆分的消息再次聚合起来，形成一个整体再向后传递
                                        //该处理器是个入站处理器
                                        .addLast(new HttpObjectAggregator(5 * 1024 * 1024))
                                        //添加入站处理器，在该处理器中执行定时任务
                                        .addLast(new EmbedHttpServerHandler(executorBiz, accessToken, bizThreadPool));
                            }
                        })
                        .childOption(ChannelOption.SO_KEEPALIVE, true);
                ChannelFuture future = bootstrap.bind(port).sync();

                logger.info(">>>>>>>>>>> xxl-job remoting server start success, nettype = {}, port = {}", EmbedServer.class, port);
                //注册执行器到调度中心
                startRegistry(appname, address);
                //等待关闭
                future.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                logger.info(">>>>>>>>>>> xxl-job remoting server stop.");
            } catch (Exception e) {
                logger.error(">>>>>>>>>>> xxl-job remoting server error.", e);
            } finally {
                try {
                    //优雅释放资源
                    workerGroup.shutdownGracefully();
                    bossGroup.shutdownGracefully();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() throws Exception {
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }
        //销毁注册执行器到调度中心的线程
        stopRegistry();
        logger.info(">>>>>>>>>>> xxl-job remoting server destroy success.");
    }


    public void startRegistry(final String appname, final String address) {
        //启动线程，注册执行器到调度中心
        ExecutorRegistryThread.getInstance().start(appname, address);
    }


    public void stopRegistry() {
        ExecutorRegistryThread.getInstance().toStop();
    }

    public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private static final Logger logger = LoggerFactory.getLogger(EmbedHttpServerHandler.class);

        // 用于调用定时任务
        private ExecutorBiz executorBiz;

        // token令牌
        private String accessToken;

        // 用于与IO线程之间进行断开
        private ThreadPoolExecutor bizThreadPool;

        public EmbedHttpServerHandler(ExecutorBiz executorBiz, String accessToken, ThreadPoolExecutor bizThreadPool) {
            this.executorBiz = executorBiz;
            this.accessToken = accessToken;
            this.bizThreadPool = bizThreadPool;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
            // 调度中心发送的消息
            String requestData = msg.content().toString(CharsetUtil.UTF_8);
            // 调度中心的地址
            String uri = msg.uri();
            // 发送请求的方法
            HttpMethod httpMethod = msg.method();

            boolean keepAlive = HttpUtil.isKeepAlive(msg);

            String accessTokenReq = msg.headers().get(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN);

            bizThreadPool.execute(() -> {
                Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);
                String responseJson = GsonTool.toJson(responseObj);
                writeResponse(ctx, keepAlive, responseJson);
            });
        }

        private Object process(HttpMethod httpMethod, String uri, String requestData, String accessTokenReq) {
            if (HttpMethod.POST != httpMethod) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
            }

            if (uri == null || uri.trim().isEmpty()) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
            }

            if (accessToken != null
                    && !accessToken.trim().isEmpty()
                    && !accessToken.equals(accessTokenReq)) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
            }

            try {
                switch (uri) {
                    case "/run":
                        // 执行定时任务
                        TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
                        return executorBiz.run(triggerParam);
                    default:
                        return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
            }
        }

        private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, String responseJson) {
            //设置响应结果
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(responseJson, CharsetUtil.UTF_8));
            //设置文本类型
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html;charset=UTF-8");
            //消息的字节长度
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            if (keepAlive) {
                //连接是存活状态
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            //开始发送消息
            ctx.writeAndFlush(response);
        }


        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error(">>>>>>>>>>> xxl-job provider netty_http server caught exception", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.channel().close();
                logger.debug(">>>>>>>>>>> xxl-job provider netty_http server close an idle channel.");
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }
}
