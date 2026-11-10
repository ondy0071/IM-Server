package utb.fai;

import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class ActiveHandlers {
    private final Set<SocketHandler> activeHandlersSet = new HashSet<>();

    synchronized void add(SocketHandler handler) {
        activeHandlersSet.add(handler);
        handler.rooms.add("public");
    }

    synchronized void remove(SocketHandler handler) {
        activeHandlersSet.remove(handler);
        handler.rooms.clear();
    }

    synchronized boolean setName(SocketHandler handler, String name) {
        String normalizedName = name.toLowerCase(Locale.ROOT);
        for (SocketHandler activeHandler : activeHandlersSet) {
            if (activeHandler != handler && activeHandler.name != null
                    && activeHandler.name.toLowerCase(Locale.ROOT).equals(normalizedName)) {
                return false;
            }
        }
        handler.name = name;
        return true;
    }

    synchronized boolean isNamed(SocketHandler handler) {
        return handler.name != null;
    }

    synchronized String getName(SocketHandler handler) {
        return handler.name;
    }

    synchronized boolean joinRoom(SocketHandler handler, String room) {
        return handler.rooms.add(room);
    }

    synchronized boolean leaveRoom(SocketHandler handler, String room) {
        return handler.rooms.remove(room);
    }

    synchronized List<String> getRooms(SocketHandler handler) {
        List<String> rooms = new ArrayList<>(handler.rooms);
        Collections.sort(rooms);
        return rooms;
    }

    synchronized void sendMessageToRoom(SocketHandler sender, String message) {
        Set<String> senderRoomky = new HashSet<>(sender.rooms);
        for (SocketHandler handler : activeHandlersSet) {
            if (handler != sender && !java.util.Collections.disjoint(handler.rooms, senderRoomky)) {
                handler.send(message);
            }
        }
    }

    synchronized boolean sendPrivateMessage(SocketHandler sender, String recipientName, String message) {
        for (SocketHandler handler : activeHandlersSet) {
            if (handler.name != null && handler.name.equalsIgnoreCase(recipientName)) {
                handler.send("[" + sender.name + "] >> " + message);
                return true;
            }
        }
        return false;
    }
}