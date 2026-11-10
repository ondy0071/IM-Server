package utb.fai;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SocketHandler {
    private static final String SET_NAME_COMMAND = "#setMyName";

    final Socket mySocket;
    final String clientID;
    final ActiveHandlers activeHandlers;
    final ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<>(20);
    final CountDownLatch startSignal = new CountDownLatch(2);
    final OutputHandler outputHandler = new OutputHandler();
    final InputHandler inputHandler = new InputHandler();
    final Set<String> rooms = new HashSet<>();
    volatile boolean inputFinished;
    String name;

    public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
        this.mySocket = mySocket;
        this.clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
        this.activeHandlers = activeHandlers;
    }

    void send(String message) {
        if (!messages.offer(message)) {
            System.err.printf("Fronta zprav klienta %s je plna, zprava se zahodi!%n", clientID);
        }
    }

    private void handleRequest(String request) {
        if (!activeHandlers.isNamed(this)) {
            String requestedName = request;
            if (isCommand(request, SET_NAME_COMMAND)) {
                requestedName = request.substring(SET_NAME_COMMAND.length()).trim();
            }
            setName(requestedName);
            return;
        }

        if (isCommand(request, SET_NAME_COMMAND)) {
            setName(request.substring(SET_NAME_COMMAND.length()).trim());
        } else if (isCommand(request, "#sendPrivate")) {
            sendPrivate(request.substring("#sendPrivate".length()).trim());
        } else if (isCommand(request, "#join")) {
            joinRoom(request.substring("#join".length()).trim());
        } else if (isCommand(request, "#leave")) {
            leaveRoom(request.substring("#leave".length()).trim());
        } else if (request.equals("#groups")) {
            send("Your rooms: " + String.join(", ", activeHandlers.getRooms(this)));
        } else {
            String senderName = activeHandlers.getName(this);
            String message = "[" + senderName + "] >> " + request;
            System.out.println(message);
            activeHandlers.sendMessageToRoom(this, message);
        }
    }

    private void setName(String requestedName) {
        if (requestedName.isEmpty() || requestedName.matches(".*\\s+.*")
                || requestedName.startsWith("#")) {
            send("Jmeno musi byt jedno slovo bez mezer. Zadejte svoje jmeno:");
            return;
        }
        if (!activeHandlers.setName(this, requestedName)) {
            send("Tohle jmeno je uz pouzite. Zadejte jine jmeno:");
            return;
        }
        send("Your name is now " + requestedName + ".");
        System.out.println(clientID + " je ted znamy jako " + requestedName);
    }

    private void sendPrivate(String commandArguments) {
        String[] parts = commandArguments.split("\\s+", 2);
        if (parts.length != 2 || parts[1].isBlank()) {
            send("Pouziti: #sendPrivate <jmeno> <zprava>");
            return;
        }
        if (!activeHandlers.sendPrivateMessage(this, parts[0], parts[1])) {
            send("Uzivatel " + parts[0] + " nebyl nalezen.");
        }
    }

    private void joinRoom(String room) {
        if (!isValidRoom(room)) {
            send("Pouziti: #join <nazev-roomky> (nazev roomky musi byt bez mezer)");
            return;
        }
        if (activeHandlers.joinRoom(this, room)) {
            send("Pristal jsi do roomky " + room + ".");
        } else {
            send("Uz jsi v roomce " + room + ".");
        }
    }

    private void leaveRoom(String room) {
        if (!isValidRoom(room)) {
            send("Pouziti: #leave <nazev-roomky> (nazev roomky musi byt bez mezer)");
            return;
        }
        if (activeHandlers.leaveRoom(this, room)) {
            send("Opustil jsi roomku " + room + ".");
        } else {
            send("Nejsi v roomce " + room + ".");
        }
    }

    private boolean isValidRoom(String room) {
        return !room.isEmpty() && !room.matches(".*\\s+.*") && !room.startsWith("#");
    }

    private boolean isCommand(String request, String command) {
        return request.equals(command) || request.startsWith(command + " ");
    }

    class OutputHandler implements Runnable {
        @Override
        public void run() {
            try {
                startSignal.countDown();
                startSignal.await();
                OutputStreamWriter writer = new OutputStreamWriter(
                        mySocket.getOutputStream(), StandardCharsets.UTF_8);
                writer.write("Jsi pripojen z " + clientID + "\r\n");
                writer.write("Zadej sve jmeno (nebo pouzij #setMyName <jmeno>):\r\n");
                writer.flush();
                while (true) {
                    String message = messages.poll(1, TimeUnit.SECONDS);
                    if (message == null && inputFinished) {
                        break;
                    }
                    if (message == null) {
                        continue;
                    }
                    writer.write(message + "\r\n");
                    writer.flush();
                }
            } catch (IOException e) {
                System.err.println("Output handler pro " + clientID + " selhal: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    mySocket.close();
                } catch (IOException e) {
                    System.err.println("Nelze zavrit spojeni pro " + clientID + ": " + e.getMessage());
                }
            }
        }
    }

    class InputHandler implements Runnable {
        @Override
        public void run() {
            try {
                startSignal.countDown();
                startSignal.await();
                activeHandlers.add(SocketHandler.this);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(mySocket.getInputStream(), StandardCharsets.UTF_8));
                String request;
                while ((request = reader.readLine()) != null) {
                    handleRequest(request.trim());
                }
            } catch (IOException e) {
                if (!mySocket.isClosed()) {
                    System.err.println("Input handler pro " + clientID + " selhal: " + e.getMessage());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inputFinished = true;
                activeHandlers.remove(SocketHandler.this);
                try {
                    mySocket.close();
                } catch (IOException e) {
                    System.err.println("Nelze zavrit spojeni pro " + clientID + ": " + e.getMessage());
                }
            }
        }
    }
}