#!/bin/bash
# uninstall-desktop.sh - Remove t3code-pi desktop application
set -e

APP_NAME="t3code-pi"
DESKTOP_DIR="$HOME/.local/share/applications"
ICON_DIR="$HOME/.local/share/icons/hicolor"
BIN_DIR="$HOME/.local/bin"

echo "╔══════════════════════════════════════════════════════╗"
echo "║       Uninstalling T3 Code (Pi) Desktop App         ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# Remove launcher script
if [ -f "$BIN_DIR/$APP_NAME" ]; then
    rm "$BIN_DIR/$APP_NAME"
    echo "   ✅ Removed: $BIN_DIR/$APP_NAME"
fi

# Remove .desktop file
if [ -f "$DESKTOP_DIR/$APP_NAME.desktop" ]; then
    rm "$DESKTOP_DIR/$APP_NAME.desktop"
    echo "   ✅ Removed: $DESKTOP_DIR/$APP_NAME.desktop"
fi

# Remove icons
echo "🎨 Removing icons..."
find "$ICON_DIR" -name "t3code.png" -delete 2>/dev/null || true
find "$ICON_DIR" -name "t3code.svg" -delete 2>/dev/null || true
echo "   ✅ Icons removed"

# Update desktop database
echo "🔄 Updating desktop database..."
if command -v update-desktop-database &>/dev/null; then
    update-desktop-database "$DESKTOP_DIR" 2>/dev/null || true
fi
if command -v gtk-update-icon-cache &>/dev/null; then
    gtk-update-icon-cache "$ICON_DIR" 2>/dev/null || true
fi

echo ""
echo "✅ T3 Code (Pi) has been uninstalled."
echo "   Note: The application files in the repository were not deleted."
