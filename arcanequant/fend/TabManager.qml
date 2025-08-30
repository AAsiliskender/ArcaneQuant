import QtQuick 2.6
import QtQuick.Controls 2.5
import QtQuick.Layouts 1.15
import QtQuick.Controls.Material

Item {
    id: tabHead
    Layout.margins: 10
    Layout.minimumWidth: 900
    Layout.minimumHeight: 150

    Rectangle {
        id: box
        anchors.fill: parent
        color: "#e0e0e0"
        radius: 8
        border.color: "#999"

        Column {
            anchors.centerIn: parent.centerIn
            spacing: 8

            Text {
                id: label
                anchors.right: parent.right
                text: tabHead.itemText
                font.pixelSize: 16
            }

            Button {
                text: "Rectangle..."
                anchors.left: parent.left
                onClicked: label.text = "Clicked!"
            }
        }
    }

    // Property so we can set text when adding this to a layout
    property string itemText: "Default"
    property string itemColour: "#272bff"

    Column {
        spacing: 100
        id: colCent
        anchors.horizontalCenter : parent.horizontalCenter

        Text {
            id: textttt
            text: "TAB MANAGER"
            font.pixelSize: 20
            anchors.horizontalCenter : parent.horizontalCenter
        }

        Button {
            text: "Click COL"
            onClicked: textttt.text = "You clicked the button!"
            anchors.horizontalCenter : parent.horizontalCenter
        }        
    }
    Row {
        anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
        anchors.leftMargin: 10; anchors.rightMargin: 10
        anchors.topMargin: 5; anchors.bottomMargin: 5
        spacing: parent.width * 0.2-70
        Button {
            text: "New Graph"
            onClicked: textttt.text = "Clicked!"
            width: Math.max(tabHead.height * 0.35, 120)
            height: Math.max(tabHead.height * 0.15, 30)
        }

        Button {
            text: "New Table"
            onClicked: textttt.text = "Clicked!"
            width: Math.max(tabHead.height * 0.35, 120)
            height: Math.max(tabHead.height * 0.15, 30)
        }
        Button {
            text: "Delete Current Tab"
            onClicked: textttt.text = "Clicked!"
            width: Math.max(tabHead.height * 0.35, 120)
            height: Math.max(tabHead.height * 0.15, 30)
        }
        Button {
            text: "Load Data"
            onClicked: textttt.text = "Clicked!"
            width: Math.max(tabHead.height * 0.35, 120)
            height: Math.max(tabHead.height * 0.15, 30)
        }
        
    }
    Row{
        anchors.bottom: box.verticalCenter; anchors.horizontalCenter: box.horizontalCenter
        anchors.bottomMargin: 5
        spacing: 5
        ToolBar {
            RowLayout {
                anchors.fill: parent

                ToolButton {
                    text: qsTr("Action 1")
                }
                ToolButton {
                    text: qsTr("Action 2")
                }

                ToolSeparator {}

                ToolButton {
                    text: qsTr("Action 3")
                }
                ToolButton {
                    text: qsTr("Action 4")
                }
                Item {
                    Layout.fillWidth: true
                }
            }
        }
    }
}