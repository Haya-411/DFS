import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DFSServer {
    private static final int PORT = 8080;// マスターサーバのポート番号
    // タイムアウトを設定(30秒)
    private static final long TIMEOUT_MS = 60_000; // 60秒
    // ファイル内容をメモリ上で管理
    private static Map<String, FileEntry> files = new ConcurrentHashMap<>();
    // 書き込み制限(同時書き込みを防ぐ): file → LockInfo
    private static Map<String, LockState> locks = new ConcurrentHashMap<>();

    static class FileEntry {// ファイル情報を管理するクラス
        String content; // ファイル内容
        int version = 0; // バージョン番号 (int)
        LocalDateTime lastSaved;
        LocalDateTime lastAccessed;

        FileEntry(String content) {
            this.content = content;
            this.lastSaved = null;
            this.lastAccessed = LocalDateTime.now();
        }
    }

    // ロック情報
    static class LockState {
        Set<String> readers = new HashSet<>();
        String writer = null;
        long lastActiveTime = System.currentTimeMillis(); // タイムアウト判定用

        void touch() {
            this.lastActiveTime = System.currentTimeMillis();
        }
    }

    public static void main(String[] args) throws IOException {
        int myPort = Integer.parseInt(args[0]);
        ServerSocket serverSocket = new ServerSocket(myPort);

        System.out.println("DFS Server started on port " + myPort);

        Socket socket = new Socket("localhost", PORT);//親サーバーへ接続
        BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(
                socket.getOutputStream(), true);

        out.println("REGISTER localhost " + args[0] + " ./"); // 親サーバーへ登録

        if(in.readLine().equals("OK")){
            System.out.println("Registered to Master Server");
        }else{
            System.out.println("Failed to register to Master Server");
        }

        // タイムアウト監視
        startTimeoutCleaner();

        while (true) {
            Socket Clientsocket = serverSocket.accept();
            new ClientHandler(Clientsocket).start();
        }
    }

    private static void startTimeoutCleaner() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            locks.forEach((filename, lock) -> {
                if (now - lock.lastActiveTime > TIMEOUT_MS) {
                    if (lock.writer != null || !lock.readers.isEmpty()) {
                        System.out.println("[Timeout] Releasing locks for: " + filename);
                        
                        synchronized (lock) {
                            lock.writer = null;
                            lock.readers.clear();
                        }
                    }
                }
            });
        }, 5, 5, TimeUnit.SECONDS);
    }

    static class ClientHandler extends Thread {
        private Socket socket;
        private String clientId;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.clientId = socket.getRemoteSocketAddress().toString();

        }

        public void run() {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(
                            socket.getOutputStream(), true)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] cmd = line.split(" ", 3);
                    switch (cmd[0]) {
                        case "OPEN":
                            handleOpen(cmd, out);
                            break;
                        case "UPDATE":
                            handleUpdate(cmd, in, out);
                            break;
                        case "CLOSE":
                            handleClose(cmd, in, out);
                            break;
                        case "KEEP_ALIVE":
                            handleKeepAlive(cmd, in, out);
                            break; // タイムアウト延長
                        case "LIST":
                            handleList(in,out);
                            break;
                        default:
                            out.println("[ERROR] Unknown command");
                    }
                }
            } catch (IOException e) {
                releaseAllLocks();
            }
        }

        private synchronized void handleOpen(String[] cmd, PrintWriter out) {

            String file = cmd[1];
            String mode = cmd[2];

            files.putIfAbsent(file, new FileEntry(""));
            locks.putIfAbsent(file, new LockState());
            files.get(file).lastAccessed = LocalDateTime.now(); // アクセス日時更新

            LockState lock = locks.get(file);
            lock.touch();

            synchronized (lock) {
                if (mode.equals("READ")) {
                    if (lock.writer != null) {
                        out.println("[ERROR] Locked for WRITE");
                        return;
                    }
                    lock.readers.add(clientId);
                } else { // WRITE,RW
                    if (lock.writer != null || !lock.readers.isEmpty()) {
                        out.println("[ERROR] Locked");
                        return;
                    }
                    lock.writer = clientId;
                }
            }

            FileEntry fe = files.get(file);
            out.println("OK " + fe.version);
            out.println(fe.content);
        }

        private synchronized void handleUpdate(
            String[] cmd, BufferedReader in, PrintWriter out) throws IOException {
            String file = cmd[1];
            int clientVersion = Integer.parseInt(cmd[2]);
            String newContent = in.readLine();
          
            LockState lock = locks.get(file);
            if (lock != null)
                lock.touch();

            FileEntry fe = files.get(file);
            if (fe.version != clientVersion) {
                out.println("CONFLICT " + fe.version);
            } else {
                fe.lastSaved = LocalDateTime.now();
                fe.content = newContent;
                fe.version++;
                out.println("OK " + fe.version);
            }
        }

        private synchronized void handleClose(String[] cmd, BufferedReader in, PrintWriter out) throws IOException {
            String file = cmd[1];
            LockState lock = locks.get(file);
            if (lock != null) {
                synchronized (lock) {
                    lock.readers.remove(clientId);
                    if (clientId.equals(lock.writer))
                        lock.writer = null;
                }
            }

            out.println("OK");
        }

        private void handleKeepAlive(String[] cmd, BufferedReader in, PrintWriter out) throws IOException {
            LockState lock = locks.get(cmd[1]);
            boolean hasWriter = false; 
            boolean hasReaders = false;

            if(lock.writer!=null) hasWriter = true;
            if(!lock.readers.isEmpty()) hasReaders = true;

            if (hasWriter || hasReaders) {
                lock.touch();
                out.println("OK");
            }else{
                out.println("[ERROR] No lock found for KEEP_ALIVE from " + clientId);
            }
            
        }

        private void handleList(BufferedReader in, PrintWriter out){
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                   
            out.println("FILES_LIST_START");//
            out.println(" Filename \t      Last Saved      \t    Last Accessed    \t Lock state");

            for (String fileName : files.keySet()) {
                
                out.print("- " + fileName);//ファイル名を送信

                    if(files.get(fileName).lastSaved != null){//最終保存日時があれば送信
                        out.print(" \t " + files.get(fileName).lastSaved.format(formatter));
                    }else{
                        out.print(" \t " + "      ********      ");
                    }

                    if(files.get(fileName).lastAccessed != null){//最終アクセス日時があれば送信
                        out.print(" \t " + files.get(fileName).lastAccessed.format(formatter));
                    }else{
                        out.print(" \t " + "      ********      ");
                    }

                    boolean hasWriter = false; 
                    boolean hasReaders = false;

                    if(locks.get(fileName).writer!=null) hasWriter = true;
                    if(!locks.get(fileName).readers.isEmpty()) hasReaders = true;
                      
                    if (hasReaders && hasWriter) {
                        out.println(" \t LOCKED (READ/WRITE)");
                    } else if (hasReaders && !hasWriter) {
                        out.println(" \t LOCKED (READ)");
                    } else if (!hasReaders && hasWriter){
                        out.println(" \t LOCKED (WRITE)");
                    }else {
                        out.println(" \t UNLOCKED");
                    }

            }
            out.println("FILES_LIST_END");

        }

        private synchronized void releaseAllLocks() {
            for (LockState lock : locks.values()) {
                lock.readers.remove(clientId);
                if (clientId.equals(lock.writer)) {
                    lock.writer = null;
                }
            }
        }
    }
}
