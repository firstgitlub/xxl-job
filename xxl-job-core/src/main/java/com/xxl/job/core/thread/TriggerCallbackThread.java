package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.executor.XxlJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by xuxueli on 16/7/22.
 *
 * //执行  执行器 到 调度中心的回调，主要是通知调度中心任务在执行器中 的执行结果的
 */
public class TriggerCallbackThread {
    private static Logger logger = LoggerFactory.getLogger(TriggerCallbackThread.class);

    private static TriggerCallbackThread instance = new TriggerCallbackThread();
    public static TriggerCallbackThread getInstance(){
        return instance;
    }

    private LinkedBlockingQueue<HandleCallbackParam> callBackQueue = new LinkedBlockingQueue<HandleCallbackParam>();

    private Thread triggerCallbackThread;
    private boolean toStop = false;
    public void start() {
        triggerCallbackThread = new Thread(new Runnable() {

            @Override
            public void run() {
                while(!toStop){
                    try {
                        HandleCallbackParam callback = getInstance().callBackQueue.take();
                        if (callback != null) {

                            // callback list param
                            List<HandleCallbackParam> callbackParamList = new ArrayList<HandleCallbackParam>();
                            int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                            callbackParamList.add(callback);

                            // valid
                            if (XxlJobExecutor.getAdminBizList()==null) {
                                logger.warn(">>>>>>>>>>>> xxl-job callback fail, adminAddresses is null, callbackParamList：{}", callbackParamList);
                                continue;
                            }

                            // callback, will retry if error
                            for (AdminBiz adminBiz: XxlJobExecutor.getAdminBizList()) {
                                try {
                                    //这里比较奇怪？？？
                                    //执行器 回调 admin，看是否执行成功？ 为了后期重试使用？
                                    //这里的意思是 当执行器执行了所有的任务以后，不管任务是否执行，执行成功与否
                                    //都会把执行结果通知给 调度中心，以便调度中心根据任务结果进行进一步的处理
                                    ReturnT<String> callbackResult = adminBiz.callback(callbackParamList);
                                    if (callbackResult!=null && ReturnT.SUCCESS_CODE == callbackResult.getCode()) {
                                        callbackResult = ReturnT.SUCCESS;
                                        logger.info(">>>>>>>>>>> xxl-job callback success, callbackParamList:{}, callbackResult:{}", new Object[]{callbackParamList, callbackResult});
                                        break;
                                    } else {
                                        logger.info(">>>>>>>>>>> xxl-job callback fail, callbackParamList:{}, callbackResult:{}", new Object[]{callbackParamList, callbackResult});
                                    }
                                } catch (Exception e) {
                                    logger.error(">>>>>>>>>>> xxl-job callback error, callbackParamList：{}", callbackParamList, e);
                                    //getInstance().callBackQueue.addAll(callbackParamList);
                                }
                            }

                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
        triggerCallbackThread.setDaemon(true);
        triggerCallbackThread.start();
    }
    public void toStop(){
        toStop = true;
    }

    public static void pushCallBack(HandleCallbackParam callback){
        getInstance().callBackQueue.add(callback);
        logger.debug(">>>>>>>>>>> xxl-job, push callback request, logId:{}", callback.getLogId());
    }

}
