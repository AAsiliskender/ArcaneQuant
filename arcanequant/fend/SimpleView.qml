import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtQuick.Controls.Material

ApplicationWindow {
    visible: true
    width: 1920
    height: 1080
    title: qsTr("ArcaneQuant")

    GridLayout {
        anchors.fill: parent
        columns: 3
        columnSpacing: 15
        rowSpacing: 10

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

        MainItem {
            Layout.alignment : Qt.AlignHCenter | Qt.AlignVCenter
            itemText: qsTr("Item 7")
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
}