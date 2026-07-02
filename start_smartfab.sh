#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Read number of peers N from first parameter, default to 2 if not provided
N=${1:-2}

# Clear previous log file for a clean E2E simulation run
if [ -f smartfab.log ]; then
    echo "[SYSTEM] Clearing previous 'smartfab.log' file..."
    rm -f smartfab.log
fi

echo "============================================="
echo "    SmartFab Automation Startup Script       "
echo "============================================="
echo "Target number of peers (N): $N"
echo "---------------------------------------------"

# 1. Start or resume the Docker Mosquitto Broker container
echo "[SYSTEM] Checking MQTT Broker status..."
if [ "$(docker ps -aq -f name=smartfab-mosquitto)" ]; then
    if [ ! "$(docker ps -q -f name=smartfab-mosquitto)" ]; then
        echo "[SYSTEM] Starting existing 'smartfab-mosquitto' Docker container..."
        docker start smartfab-mosquitto
    else
        echo "[SYSTEM] 'smartfab-mosquitto' container is already running."
    fi
else
    echo "[SYSTEM] Creating and launching a new 'smartfab-mosquitto' Docker container..."
    docker run -d --name smartfab-mosquitto -p 1883:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf
fi

# Give Docker a second to initialize the listener
sleep 1

# 2. Build Java classes to guarantee fresh binaries
echo "[SYSTEM] Compiling Java classes..."
./gradlew compileJava compileTestJava

# 3. Detect available terminal emulator
echo "[SYSTEM] Detecting desktop environment terminal..."
if command -v gnome-terminal >/dev/null 2>&1; then
    TERM_EMU="gnome-terminal"
elif command -v xfce4-terminal >/dev/null 2>&1; then
    TERM_EMU="xfce4-terminal"
elif command -v konsole >/dev/null 2>&1; then
    TERM_EMU="konsole"
else
    TERM_EMU="xterm"
fi
echo "[SYSTEM] Using terminal emulator: $TERM_EMU"

# Helper to open a command in a new terminal with optional geometry
launch_terminal() {
    local title="$1"
    local command="$2"
    local geom="$3"
    
    local geom_flag=""
    if [ -n "$geom" ]; then
        if [ "$TERM_EMU" = "xterm" ]; then
            geom_flag="-geometry $geom"
        else
            geom_flag="--geometry=$geom"
        fi
    fi
    
    if [ "$TERM_EMU" = "gnome-terminal" ]; then
        env GDK_BACKEND=x11 gnome-terminal $geom_flag --title="$title" -- bash -c "$command; exec bash"
    elif [ "$TERM_EMU" = "xfce4-terminal" ]; then
        env GDK_BACKEND=x11 xfce4-terminal $geom_flag --title="$title" -e "bash -c '$command; exec bash'"
    elif [ "$TERM_EMU" = "konsole" ]; then
        konsole $geom_flag --title="$title" -e "bash -c '$command; exec bash'"
    else
        xterm $geom_flag -T "$title" -e "bash -c '$command; exec bash'"
    fi
}

# 4. Start Administration Server
echo "[SYSTEM] Launching Admin Server in a new terminal..."
launch_terminal "SmartFab - Admin Server" "./gradlew bootRun"

# Wait for Tomcat to spin up before peers register
echo "[SYSTEM] Waiting 6 seconds for Admin Server to boot up..."
sleep 6

# Create or touch log file so tail command starts correctly
touch smartfab.log
# Start Unified Log Viewer
echo "[SYSTEM] Launching Unified Log Viewer (tail -f smartfab.log)..."
launch_terminal "SmartFab - Unified Log Viewer" "tail -f smartfab.log"

# 5. Start N Peer Nodes in a single tabbed window
echo "[SYSTEM] Launching Peer Nodes as tabs inside a single window..."
if [ "$TERM_EMU" = "gnome-terminal" ]; then
    PEER_COMMAND="env GDK_BACKEND=x11 gnome-terminal"
    for ((i=1; i<=N; i++))
    do
        PORT=$((5000 + i))
        if [ $i -eq 1 ]; then
            PEER_COMMAND="$PEER_COMMAND --window --title=\"SmartFab - Peer Node $i (Port $PORT)\" --command=\"bash -c './gradlew runPeer -Pargs=\\\"$i 127.0.0.1 $PORT http://localhost:8080 tcp://localhost:1883\\\"; exec bash'\""
        else
            PEER_COMMAND="$PEER_COMMAND --tab --title=\"SmartFab - Peer Node $i (Port $PORT)\" --command=\"bash -c './gradlew runPeer -Pargs=\\\"$i 127.0.0.1 $PORT http://localhost:8080 tcp://localhost:1883\\\"; exec bash'\""
        fi
    done
    eval "$PEER_COMMAND"
elif [ "$TERM_EMU" = "xfce4-terminal" ]; then
    PEER_COMMAND="env GDK_BACKEND=x11 xfce4-terminal"
    for ((i=1; i<=N; i++))
    do
        PORT=$((5000 + i))
        if [ $i -eq 1 ]; then
            PEER_COMMAND="$PEER_COMMAND --window --title=\"SmartFab - Peer Node $i (Port $PORT)\" -e \"bash -c './gradlew runPeer -Pargs=\\\"$i 127.0.0.1 $PORT http://localhost:8080 tcp://localhost:1883\\\"; exec bash'\""
        else
            PEER_COMMAND="$PEER_COMMAND --tab --title=\"SmartFab - Peer Node $i (Port $PORT)\" -e \"bash -c './gradlew runPeer -Pargs=\\\"$i 127.0.0.1 $PORT http://localhost:8080 tcp://localhost:1883\\\"; exec bash'\""
        fi
    done
    eval "$PEER_COMMAND"
else
    # Fallback for emulators that do not support multi-tab commands (e.g., xterm, konsole)
    for ((i=1; i<=N; i++))
    do
        PORT=$((5000 + i))
        echo "[SYSTEM] Launching Peer Node $i on port $PORT..."
        launch_terminal "SmartFab - Peer Node $i (Port $PORT)" "./gradlew runPeer -Pargs=\"$i 127.0.0.1 $PORT http://localhost:8080 tcp://localhost:1883\""
        sleep 1
    done
fi

# 6. Start Administration Client CLI
echo "[SYSTEM] Launching Admin Client CLI..."
launch_terminal "SmartFab - Admin Client CLI" "./gradlew runClient --console=plain"

echo "---------------------------------------------"
echo "[SYSTEM] All components successfully started!"
echo "============================================="
