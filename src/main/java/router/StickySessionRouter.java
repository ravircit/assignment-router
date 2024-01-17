package router;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Setter
class Message {
    private String transactionId;
    private String content;

    public Message(String s, String s1) {
        this.transactionId =s;
        this.content=s1;
    }
}

@Getter
@Setter
class Worker {
    private String workerId;

    public Worker(String s) {
        this.workerId=s;
    }
}

@Getter
@Setter
class Router {
    private BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    private Map<String, String> stickySessions = new HashMap<>();  // Map sessionId to workerId
    private Map<String, Worker> workerPool = new HashMap<>();  // Map workerId to Worker instance

    public void addWorker(Worker worker) {
        workerPool.put(worker.getWorkerId(), worker);
    }

    public void processMessages() {
        while (true) {
            try {
                Message message = messageQueue.take();
                routeMessage(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void routeMessage(Message message) {
        String traxnId = message.getTransactionId();
        String randomWorkerId = stickySessions.get(traxnId);

        if (randomWorkerId == null) {
            randomWorkerId = getRandomWorkerId();
//            workerId = workerPool.keySet().iterator().next();
            stickySessions.put(traxnId, randomWorkerId);
        }

        Worker worker = workerPool.get(randomWorkerId);
        sendMessageToWorker(message, worker);
    }

    private String getRandomWorkerId() {
        int hashCode = stickySessions.keySet().hashCode();
        Random random = new Random(hashCode);
        String[] workerIds = workerPool.keySet().toArray(new String[0]);
        return workerIds[random.nextInt(workerIds.length)];
    }

    private void sendMessageToWorker(Message message, Worker worker) {
        // Implement logic to send the message to the specified worker
        System.out.println("Routing message to worker " + worker.getWorkerId() + ": " + message.getContent());
    }
}

public class StickySessionRouter {
    public static void main(String[] args) {
        Router router = new Router();

        // Add workers to the pool
        Worker worker1 = new Worker("Worker-1");
        Worker worker2 = new Worker("Worker-2");
        Worker worker3 = new Worker("Worker-3");
        router.addWorker(worker1);
        router.addWorker(worker2);
        router.addWorker(worker3);

        // Start processing messages
        Thread routerThread = new Thread(router::processMessages);
        routerThread.start();

        // Example: Add messages to the queue
        router.getMessageQueue().add(new Message("traxn-1", "Message 1"));
        router.getMessageQueue().add(new Message("traxn-2", "Message 2"));
        router.getMessageQueue().add(new Message("traxn-1", "Message 3"));
//        for (int i = 0; i < 10; i++) {
//            String sessionId = "session-" + (i % 2);  // Use 3 different sessions
//            String content = "Message " + (i + 1);
//            router.getMessageQueue().add(new Message(sessionId, content));
//        }
    }
}
