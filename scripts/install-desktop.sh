#!/bin/bash
# install-desktop.sh - Install t3code-pi as a desktop application on Fedora/Linux
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="t3code-pi"
APP_DISPLAY_NAME="T3 Code (Pi)"
APP_COMMENT="Web GUI for the Pi coding agent"
ICON_NAME="t3code"

# Directories
DESKTOP_DIR="$HOME/.local/share/applications"
ICON_DIR="$HOME/.local/share/icons/hicolor"
BIN_DIR="$HOME/.local/bin"

echo "╔══════════════════════════════════════════════════════╗"
echo "║       Installing T3 Code (Pi) Desktop App           ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# Check for bun
if ! command -v bun &>/dev/null; then
    if [ -f "$HOME/.bun/bin/bun" ]; then
        export PATH="$HOME/.bun/bin:$PATH"
    else
        echo "❌ Bun not found. Install it with:"
        echo "   curl -fsSL https://bun.sh/install | bash"
        exit 1
    fi
fi

echo "📦 Building the application..."
cd "$SCRIPT_DIR"
bun run build 2>&1 | tail -5
echo ""

# Find the Electron binary
ELECTRON_BINARY="$SCRIPT_DIR/node_modules/.bun/electron@40.9.3/node_modules/electron/dist/electron"
if [ ! -f "$ELECTRON_BINARY" ]; then
    echo "❌ Electron binary not found at: $ELECTRON_BINARY"
    echo "   Run 'bun install' first."
    exit 1
fi

# Create launcher script
echo "🔧 Creating launcher script..."
mkdir -p "$BIN_DIR"

cat > "$BIN_DIR/$APP_NAME" << LAUNCHER
#!/bin/bash
# T3 Code (Pi) - Desktop Application Launcher

export BUN_INSTALL="\$HOME/.bun"
export PATH="\$BUN_INSTALL/bin:\$PATH"

T3CODE_ROOT="$SCRIPT_DIR"
ELECTRON="$ELECTRON_BINARY"
MAIN_CJS="\$T3CODE_ROOT/apps/desktop/dist-electron/main.cjs"

# Set dev root so the app can find its resources
export T3CODE_DEV_ROOT="\$T3CODE_ROOT"

# Launch Electron app
exec "\$ELECTRON" --t3code-dev-root="\$T3CODE_ROOT" "\$MAIN_CJS" "\$@"
LAUNCHER

chmod +x "$BIN_DIR/$APP_NAME"
echo "   ✅ Created: $BIN_DIR/$APP_NAME"

# Create .desktop file
echo "🖥️  Creating desktop entry..."
mkdir -p "$DESKTOP_DIR"

cat > "$DESKTOP_DIR/$APP_NAME.desktop" << DESKTOP
[Desktop Entry]
Name=$APP_DISPLAY_NAME
Comment=$APP_COMMENT
Exec=$BIN_DIR/$APP_NAME %U
Icon=$SCRIPT_DIR/apps/desktop/resources/icon.png
Type=Application
Categories=Development;IDE;
Terminal=false
StartupWMClass=t3code
MimeType=x-scheme-handler/t3code;
Keywords=coding;agent;ai;pi;
DESKTOP

echo "   ✅ Created: $DESKTOP_DIR/$APP_NAME.desktop"

# Copy icon if it exists
ICON_SRC="$SCRIPT_DIR/apps/desktop/resources/icon.png"
if [ -f "$ICON_SRC" ]; then
    echo "🎨 Installing icon..."
    for SIZE in 16 32 48 64 128 256 512; do
        ICON_DIR_SIZE="$ICON_DIR/${SIZE}x${SIZE}/apps"
        mkdir -p "$ICON_DIR_SIZE"
        cp "$ICON_SRC" "$ICON_DIR_SIZE/$ICON_NAME.png" 2>/dev/null || true
    done
    # Also install to scalable if we have an SVG
    ICON_SVG="$SCRIPT_DIR/apps/desktop/assets/icon.svg"
    if [ -f "$ICON_SVG" ]; then
        mkdir -p "$ICON_DIR/scalable/apps"
        cp "$ICON_SVG" "$ICON_DIR/scalable/apps/$ICON_NAME.svg"
    fi
    echo "   ✅ Icon installed"
fi

# Update desktop database
echo "🔄 Updating desktop database..."
if command -v update-desktop-database &>/dev/null; then
    update-desktop-database "$DESKTOP_DIR" 2>/dev/null || true
fi
if command -v gtk-update-icon-cache &>/dev/null; then
    gtk-update-icon-cache "$ICON_DIR" 2>/dev/null || true
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║            ✅ Installation Complete!                 ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║                                                      ║"
echo "║  You can now:                                        ║"
echo "║  • Find 'T3 Code (Pi)' in your application menu      ║"
echo "║  • Run: t3code-pi                                     ║"
echo "║  • Run: $BIN_DIR/$APP_NAME                            ║"
echo "║                                                      ║"
echo "║  To uninstall, run:                                  ║"
echo "║  $SCRIPT_DIR/scripts/uninstall-desktop.sh             ║"
echo "║                                                      ║"
echo "╚══════════════════════════════════════════════════════╝"
