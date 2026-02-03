import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtQuick.Controls.Material

ApplicationWindow {
    id: mainWindow
    visible: true
    width: 1920
    height: 1080
    title: qsTr("ArcaneQuant")

    property alias rootWindow: mainWindow

    GridLayout {
        anchors.fill: parent
        columns: 3
        columnSpacing: 5
        rowSpacing: 5

        DataManifestController {
            id: dmController
            Layout.alignment : Qt.AlignHCenter | Qt.AlignVCenter
        }
        TabManager {
            Layout.alignment : Qt.AlignHCenter | Qt.AlignVCenter
            itemText: qsTr("TabManager")
        }
        MainItem {

            itemText: qsTr("Item 2")
        }

        DataController {
            id: dController
        }
        TabView {

        }
        MainItem {
            Layout.alignment : Qt.AlignHCenter | Qt.AlignVCenter
            itemText: qsTr("Item 6")
        }

        SQLController {
            Layout.alignment : Qt.AlignHCenter | Qt.AlignVCenter
        }
        MainItem {
            Layout.alignment : Qt.AlignHCenter | Qt.AlignVCenter
            itemText: qsTr("Item 8")
        }
        MainItem {
            Layout.alignment : Qt.AlignHCenter | Qt.AlignVCenter
            itemText: qsTr("Item 9")
        }
    }

    // Global confirmation popup
    Popup {
        id: globalConfirm
        modal: true
        focus: true
        anchors.centerIn: parent
        width: 400
        height: 200

        property string title: ""
        property string message: ""
        property var onConfirm: null

        contentItem: Column {
            spacing: 10
            padding: 0

            Text {
                width: globalConfirm.width*0.9
                anchors.horizontalCenter: parent.horizontalCenter
                text: globalConfirm.title
                wrapMode: Text.WordWrap
                horizontalAlignment: Text.AlignHCenter
                font.pixelSize: 20
                font.weight: Font.DemiBold
            }
            Text {
                height: 80
                width: globalConfirm.width*0.9
                anchors.horizontalCenter: parent.horizontalCenter
                text: globalConfirm.message
                horizontalAlignment: Text.AlignHCenter
                wrapMode: Text.WordWrap
            }
            Row {
                spacing: 10
                anchors.horizontalCenter: parent.horizontalCenter

                Button {
                    text: qsTr("Cancel")
                    onClicked: globalConfirm.close()
                    
                }
                
                Item { height: 1; width: 20 }

                Button {
                    text: qsTr("Confirm")
                    Material.background: "red"
                    Material.foreground: "white"
                    onClicked: {
                        if (globalConfirm.onConfirm) {
                            globalConfirm.onConfirm()
                        }
                        globalConfirm.close()
                    }
                }
            }
        }
        // Reset to prevent unintended reuse
        onClosed: {
            title = ""
            message = ""
            onConfirm = function() {}
        }
    }

    // Global destructive action confirmation  popup
    Popup {
        id: globalDestructConfirm
        modal: true
        focus: true
        anchors.centerIn: parent
        width: 400
        height: 200        

        property string title: ""
        property string message: ""
        property var onConfirm: null
        property var theme: "basic"

        contentItem: Column {
            spacing: 10
            padding: 0

            Text {
                width: globalDestructConfirm.width*0.9
                anchors.horizontalCenter: parent.horizontalCenter
                text: globalDestructConfirm.title
                wrapMode: Text.WordWrap
                horizontalAlignment: Text.AlignHCenter
                font.pixelSize: 20
                font.weight: Font.DemiBold
            }
            Text {
                height: 80
                width: globalDestructConfirm.width*0.9
                anchors.horizontalCenter: parent.horizontalCenter
                text: globalDestructConfirm.message
                horizontalAlignment: Text.AlignHCenter
                wrapMode: Text.WordWrap
            }
            Row {
                spacing: 10
                anchors.horizontalCenter: parent.horizontalCenter

                Button {
                    text: qsTr("Cancel")
                    onClicked: {
                        globalDestructConfirm.close()

                        // Reset theme, title, message after close
                        globalDestructConfirm.theme = "basic"
                        globalDestructConfirm.title = ""
                        globalDestructConfirm.message = ""
                    }
                    
                }
                
                Item { height: 1; width: 20 }

                Button {
                    text: if (globalDestructConfirm.theme == "extreme") {
                        qsTr("CONFIRM")
                    } else {
                        qsTr("Confirm")
                    }
                    Material.background: if (globalDestructConfirm.theme == "extreme") {
                        "yellow"
                    } else {
                        "red"
                    }
                    Material.foreground: if (globalDestructConfirm.theme == "extreme") {
                        "red"
                    } else {
                        "white"
                    }
                    onClicked: {
                        if (globalDestructConfirm.onConfirm) {
                            globalDestructConfirm.onConfirm()
                        }
                        globalDestructConfirm.close()
                    }
                }
            }
        }
        // Reset to prevent unintended reuse
        onClosed: {
            title = ""
            message = ""
            theme = "basic"
            onConfirm = function() {}
        }

        background: Rectangle {
            anchors.fill: parent
            radius: if (globalDestructConfirm.theme == "extreme") {
                radius: 0
            } else {
                radius: 4
            }
            color: if (globalDestructConfirm.theme == "extreme") {
                "red"
            } else {
                "white"
            }
            border.color: if (globalDestructConfirm.theme == "extreme") {
                "yellow"
            } else {
                "red"
            }
            border.width: if (globalDestructConfirm.theme == "extreme") {
                7
            } else {
                2
            }
        }
    }
}