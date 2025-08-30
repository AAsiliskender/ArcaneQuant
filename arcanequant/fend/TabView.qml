import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtQuick.Controls.Material

ColumnLayout {
    id: centreBox
    Layout.alignment: Qt.AlignCenter
    Layout.fillWidth: true
    Layout.fillHeight: true
    Layout.minimumWidth: 1000
    Layout.minimumHeight: 500
    clip: true

    Frame {
        Layout.fillWidth: true
        Layout.fillHeight: true
        Layout.margins: 15
        background: Rectangle {
            color: "transparent"
            border.color: "darkblue"
            border.width: 1
            radius: 6
        }

        // --- Tab header ---
        TabBar {
            id: tabBar
            anchors.left: parent.left; anchors.right: parent.right; anchors.bottom: parent.top
            height: 40
            clip: true


            TabButton {
                text: "Home"; onClicked: stackLayout.currentIndex = 0
                height : parent.height
            }
            TabButton {
                text: "Data Manifest"; onClicked: stackLayout.currentIndex = 1
                height : parent.height
            }

            Repeater {
                model: tabModel
                TabButton {
                    text: name
                    height : parent.height
                    onClicked: stackLayout.currentIndex = 2 + index // Using repeater index
                }
            }
        }

        // --- Tab content ---
        StackLayout {
            id: stackLayout
            anchors.fill: parent
            Layout.fillWidth: true
            Layout.fillHeight: true

            // Index 0
            Rectangle {
                color: "lightblue"
                Layout.fillWidth: true
                Layout.fillHeight: true
                Label {
                    text: "This is the Home tab"
                    anchors.centerIn: parent
                }
            }

            // Index 1
            GridLayout {
                id: grid
                rows: 2
                columns: 2
                Layout.fillWidth: true
                Layout.fillHeight: true
                //width: stackLayout.width
                //height: stackLayout.height
                rowSpacing: 0
                columnSpacing: 0

                Rectangle {         // Spacer for column header
                    Layout.row: 0; Layout.column: 0
                    Layout.preferredWidth: vHeader.width; Layout.preferredHeight: hHeader.height
                    color: "transparent"
                }

                // Shows column indices
                HorizontalHeaderView {
                    id: hHeader
                    Layout.row: 0; Layout.column: 1
                    Layout.fillWidth: true // Using fillWidth here stops overflow (as its implictWidth is limited to layout instead of whatever the columns want to be)
                    syncView: tableView
                    clip: true
                    delegate: Rectangle {
                        implicitHeight: 30
                        implicitWidth: 55
                        border.color: "#ff7300"
                        color: (row % 2 === 0 ? "#32ace4" : "#8738ee")

                        Text {
                            text: model.display
                            horizontalAlignment: Text.AlignHCenter
                            padding: 4
                        }
                    }
                }

                // Shows row indices
                VerticalHeaderView {
                    id: vHeader
                    Layout.row: 1
                    Layout.column: 0
                    Layout.fillHeight: true // Using fillHeight here stops overflow (as its implicitHeight is limited to layout instead of whatever the rows want to be)
                    syncView: tableView
                    clip: true
                    delegate: Rectangle {
                        implicitHeight: 30
                        implicitWidth: 75

                        border.color: "#0400ff"
                        color: (row % 2 === 0 ? "#e4d832" : "#e3ff7d")

                        Text {
                            text: model.display
                            horizontalAlignment: Text.AlignHCenter
                            padding: 4
                        }
                    }
                }

                // Data Content area
                TableView {
                    id: tableView
                    Layout.row: 1
                    Layout.column: 1

                    Layout.fillWidth: true
                    Layout.fillHeight: true
                    
                    clip: true
                    boundsBehavior: Flickable.StopAtBounds
                    ScrollBar.horizontal: ScrollBar { policy: ScrollBar.AsNeeded }
                    ScrollBar.vertical: ScrollBar { policy: ScrollBar.AsNeeded }
                    
                    model: dfModel

                    // Create columns dynamically from model headers
                    //Component.onCompleted: {
                    //    for (let i = 0; i < model.columnCount(); i++) {
                    //        tableView.addColumn(
                    //            Qt.createQmlObject(
                    //                'import QtQuick.Controls 2.15; TableViewColumn { role: "display"; title: "' + model.headerData(i, Qt.Horizontal) + '"; width: 60; height: 200 }',
                    //                tableView
                    //            )
                    //        )
                    //    }
                    //}

                    delegate: Rectangle {
                        implicitHeight: 30
                        implicitWidth: 55
                        border.color: "#ddd"
                        color: (row % 2 === 0 ? "#d1d1d1" : "#ffffff")

                        Text {
                            text: model.display   // works now
                            padding: 4
                        }
                    }
                }
            }

            // Index 2+
            // Used whenever stockdata is added
            Repeater {
                id: tabmaker
                model: tabModel

                delegate: GridLayout {
                        id: grid_repeat
                        rows: 2
                        columns: 2
                        Layout.fillWidth: true
                        Layout.fillHeight: true
                        rowSpacing: 0
                        columnSpacing: 0

                        Rectangle {         // Spacer for column header
                            Layout.row: 0; Layout.column: 0
                            Layout.preferredWidth: vHeader_repeat.width; Layout.preferredHeight: hHeader_repeat.height
                            color: "transparent"
                        }

                        // Shows column indices
                        HorizontalHeaderView {
                            id: hHeader_repeat
                            Layout.row: 0; Layout.column: 1
                            Layout.fillWidth: true // Using fillWidth here stops overflow (as its implictWidth is limited to layout instead of whatever the columns want to be)
                            syncView: table_repeat
                            clip: true
                            delegate: Rectangle {
                                implicitHeight: 40
                                implicitWidth: 70
                                border.color: "#ff7300"
                                color: (row % 2 === 0 ? "#32ace4" : "#8738ee")

                                Text {
                                    text: model.display
                                    horizontalAlignment: Text.AlignHCenter
                                    padding: 4
                                }
                            }
                        }

                        // Shows row indices
                        VerticalHeaderView {
                            id: vHeader_repeat
                            Layout.row: 1
                            Layout.column: 0
                            Layout.fillHeight: true // Using fillHeight here stops overflow (as its implictHeight is limited to layout instead of whatever the rows want to be)
                            syncView: table_repeat
                            clip: true
                            delegate: Rectangle {
                                implicitHeight: 32
                                implicitWidth: 40

                                border.color: "#0400ff"
                                color: (row % 2 === 0 ? "#e4d832" : "#e3ff7d")

                                Text {
                                    text: model.display
                                    horizontalAlignment: Text.AlignHCenter
                                    padding: 4
                                }
                            }
                        }
                            
                        TableView {
                            id: table_repeat
                            clip: true
                            ScrollBar.horizontal: ScrollBar { policy: ScrollBar.AsNeeded }
                            ScrollBar.vertical: ScrollBar { policy: ScrollBar.AsNeeded }
                            boundsBehavior: Flickable.StopAtBounds

                            model: dfModel

                            Layout.fillWidth: true
                            Layout.fillHeight: true

                            delegate: Rectangle {
                                implicitHeight: 32
                                implicitWidth: 150
                                border.color: "#ddd"
                                color: (row % 2 === 0 ? "#d1d1d1" : "#ffffff")

                                Text {
                                    text: model.display   // works now
                                    padding: 4
                                }
                            }
                        }
                    }
                
                
            }
        }
        
        
    }
}
