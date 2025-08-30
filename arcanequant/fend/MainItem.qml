import QtQuick 2.6
import QtQuick.Controls 2.5
import QtQuick.Layouts 1.15
import QtQuick.Controls.Material

Item {
    id: root
    Layout.margins: 10
    width: 400
    height: 200

    Rectangle {
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
                text: root.itemText
                font.pixelSize: 16
            }

            Button {
                text: "Rectangle..."
                anchors.left: parent.left
                onClicked: label.text = "Clicked!"
            }

            Button {
                text: "Load Data TEST"
                onClicked: backend._loadData_fromcsv("MSFT", 5, "2022-01", true, true)
            }
        }
    }

    // Property so we can set text when adding this to a layout
    property string itemText: "Default"
    property string itemColour: "#ffffff"

    Column {
        spacing: 100
        id: colCent
        anchors.horizontalCenter : parent.horizontalCenter

        Text {
            id: textttt
            text: "TEST COL IN ITEM!"
            font.pixelSize: 20
            anchors.horizontalCenter : parent.horizontalCenter
        }

        Button {
            text: "Click COL"
            onClicked: textttt.text = "You clicked the button!"
            anchors.horizontalCenter : parent.horizontalCenter
        }        
    }
}