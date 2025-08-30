import QtQuick 2.6
import QtQuick.Controls 2.5
import QtQuick.Layouts 1.15
import QtQuick.Controls.Material
import Qt.labs.qmlmodels

// Note I use //RelativeSize as a market to distinguish percentage sizes so I can modify easily


Rectangle {
    id: mainBox
    Layout.margins: 10
    Layout.alignment: Qt.AlignCenter
    Layout.minimumHeight: 570
    color: itemColour
    radius: 8
    border.color: itemBorderColour
    Layout.minimumWidth: 400 //Math.max(parent.width * 0.2, 400) //RelativeSize
    Layout.fillHeight: true
    //Layout.minimumHeight: Math.max(parent.height * 0.15, 600) //RelativeSize

    // Property so we can set text when adding this to a layout
    property string itemText: qsTr("Row below load extract data and have 3 small tables for lists of ticker, interval, month")
    property string itemColour: "#e0e0e0"
    property string itemBorderColour: "#dd0000"
    property string subItemBorderColour: "#0400ff"

    
    Rectangle{
        anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
        color: itemColour
        height: title.height/2
        width: title.width*1.1
        Text {
            id: title
            anchors.verticalCenter: parent.top; anchors.horizontalCenter: parent.horizontalCenter
            text: qsTr("Data Controller")
            font.pixelSize: 18
            font.weight: Font.DemiBold
            z: 1
        }
    }

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 5
        Rectangle {
            id: boxTop
            Layout.preferredHeight: parent.height/6
            Layout.minimumHeight: 95
            Layout.fillWidth: true
            color: itemColour
            radius: 8
            border.color: subItemBorderColour
            Layout.margins: 5


            Row {
                anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                anchors.topMargin: 20
                spacing: 5
                Text { text: qsTr("Save setting:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
                // Combo box for selecting save options
                ComboBox {
                    id: saveops
                    model: [qsTr('Direct'), qsTr('Database'), qsTr('Both')]
                    width: 95
                    height: 30
                    currentIndex: 0
                    rightPadding: 30
                    leftPadding: -5
                    anchors.verticalCenter: parent.verticalCenter
                    // TODO: ADD OPERATION HERE
                }

                Item { height:1; width: 30 } // Spacer item
                
                Text { text: qsTr("Load setting:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
                // Combo box for selecting load options
                ComboBox {
                    id: loadops
                    model: [qsTr('Direct'), qsTr('Database')]
                    width: 95
                    height: 30
                    currentIndex: 0
                    rightPadding: 30
                    leftPadding: -5
                    anchors.verticalCenter: parent.verticalCenter

                }
            }
            
            
            
            
            Row {
                anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
                anchors.leftMargin: 10; anchors.rightMargin: 10
                anchors.topMargin: 5; anchors.bottomMargin: 5
                spacing: parent.width * 0.2-70 //RelativeSize

                // Extract data (arbitrary ticker, months, intervals)
                Button {
                    text: qsTr("Extract Data")
                    onClicked: dirLabel.text = qsTr("Clicked!")
                    width: Math.max(parent.width * 0.09, 120) //RelativeSize
                    height: 30 //Math.max(parent.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }
                // Extracting only unit data (1 ticker, month period, interval)
                Button {
                    text: qsTr("Extract Unit Data")
                    onClicked: {
                        //balls

                    }
                    width: Math.max(parent.width * 0.09, 120) //RelativeSize
                    height: 30 //Math.max(parent.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }         
            }
        }

        Rectangle {
            id: boxMid
            Layout.preferredHeight: parent.height/5
            Layout.minimumHeight: 110
            Layout.fillWidth: true
            color: itemColour
            radius: 8
            border.color: subItemBorderColour
            Layout.margins: 5

            property bool unitAvail: false

            Rectangle{
                anchors.verticalCenter: boxMid.top; anchors.horizontalCenter: parent.horizontalCenter
                color: itemColour
                height: udeHeader.height
                width: udeHeader.width*1.1
                Text {
                    id: udeHeader
                    anchors.centerIn: parent
                    text: qsTr("Unit Data Extractor")
                    font.pixelSize: 18
                    font.weight: Font.DemiBold
                }
            }

            Row {
                id: unitRowHead
                anchors.top: parent.top; anchors.left: parent.left
                anchors.topMargin: 20
                spacing: 5

                Item { height:1; width: 38 } // Spacer item

                Text { text: qsTr("Ticker:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}

                Item { height:1; width: 58 } // Spacer item
                
                Text { text: qsTr("Interval:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}

                Item { height:1; width: 58 } // Spacer item

                Text { text: qsTr("Year-Month:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
            }

            Row {
                anchors.top: unitRowHead.bottom; anchors.horizontalCenter: parent.horizontalCenter
                anchors.topMargin: 5
                spacing: 5

                // Combo box for selecting save options
                ComboBox {
                    id: existingTickers
                    model: backend.allTickers
                    textRole: "modelData" // Required, makes combobox read the list
                    width: 80
                    height: 30
                    currentIndex: 0
                    rightPadding: 30
                    leftPadding: -5
                    anchors.verticalCenter: parent.verticalCenter
                    onCurrentValueChanged: {
                        boxMid.unitAvail = backend._check_DataDM(existingTickers.currentValue, existingIntervals.currentValue, existingYears.currentValue+"-"+existingMonths.currentValue)
                    }
                }

                Item { height:1; width: 25 } // Spacer item
                
                // Combo box for selecting interval
                ComboBox {
                    id: existingIntervals
                    model: [1, 5, 15, 30, 60]
                    width: 60
                    height: 30
                    currentIndex: 0
                    rightPadding: 30
                    leftPadding: -5
                    anchors.verticalCenter: parent.verticalCenter
                    onCurrentValueChanged: {
                        boxMid.unitAvail = backend._check_DataDM(existingTickers.currentValue, existingIntervals.currentValue, existingYears.currentValue+"-"+existingMonths.currentValue)
                    }
                }

                Item { height:1; width: 25 } // Spacer item

                // Combo boxes for selecting year-month
                ComboBox {
                    id: existingYears
                    model: backend.allYears
                    textRole: "modelData" // Required, makes combobox read the list
                    width: 70
                    height: 30
                    currentIndex: 0
                    rightPadding: 30
                    leftPadding: -5
                    anchors.verticalCenter: parent.verticalCenter
                    onCurrentValueChanged: {
                        boxMid.unitAvail = backend._check_DataDM(existingTickers.currentValue, existingIntervals.currentValue, existingYears.currentValue+"-"+existingMonths.currentValue)
                    }
                }
                ComboBox {
                    id: existingMonths
                    model: backend.allMonths
                    textRole: "modelData" // Required, makes combobox read the list
                    width: 55
                    height: 30
                    currentIndex: 0
                    rightPadding: 30
                    leftPadding: -5
                    anchors.verticalCenter: parent.verticalCenter
                    onCurrentValueChanged: {
                        boxMid.unitAvail = backend._check_DataDM(existingTickers.currentValue, existingIntervals.currentValue, existingYears.currentValue+"-"+existingMonths.currentValue)
                    }
                }
                
            }

            Row {
                anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
                anchors.leftMargin: 10; anchors.rightMargin: 10
                anchors.bottomMargin: 10
                height: 30

                Rectangle {
                    anchors.verticalCenter: parent.verticalCenter
                    width: 25
                    height: width
                    radius: width/2
                    color: if (boxMid.unitAvail == false) {
                        "red"
                    } else if (loadops.currentValue == 'Database' && !dmController.sqlConnected) {
                        "orange"
                    } else {
                        "green"
                    }
                    
                }

                Item { height:1; width: 35 } // Spacer item

                // Extracting only unit data (1 ticker, month period, interval)
                Button {
                    anchors.verticalCenter: parent.verticalCenter
                    text: qsTr("Extract Unit Data")
                    enabled: {
                        if (loadops.currentValue == 'Direct') {
                            boxMid.unitAvail
                        } else if (loadops.currentValue == 'Database' ) {
                            boxMid.unitAvail
                        } else { false }
                    }

                    onClicked: {
                        if (loadops.currentValue == 'Direct') {
                            backend._loadData_fromcsv(existingTickers.currentValue, existingIntervals.currentValue, existingYears.currentValue+"-"+existingMonths.currentValue, true, metaCheck.checked)
                        } else {
                            backend._loadData_fromsql(existingTickers.currentValue, existingIntervals.currentValue, existingYears.currentValue+"-"+existingMonths.currentValue, true, metaCheck.checked)
                        }
                    }
                    width: Math.max(parent.width * 0.09, 120) //RelativeSize
                    height: 30 //parent.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }

                Item { height:1; width: 15 } // Spacer item

                CheckBox {
                    id: metaCheck
                    anchors.verticalCenter: parent.verticalCenter
                    checked: false
                    text: qsTr("Meta Data")
                }
            }
        }

        Rectangle {
            id: boxBot
            //Layout.preferredHeight: mainBox.height/2 - 10
            Layout.fillHeight: true
            Layout.fillWidth: true
            Layout.minimumHeight: 315
            color: itemColour
            radius: 8
            border.color: subItemBorderColour
            Layout.margins: 5

            Rectangle{
                anchors.verticalCenter: boxBot.top; anchors.horizontalCenter: parent.horizontalCenter
                color: itemColour
                height: dedHeader.height
                width: dedHeader.width*1.1
                Text {
                    id: dedHeader
                    anchors.centerIn: parent
                    text: qsTr("Data Extractor/Downloader")
                    font.pixelSize: 18
                    font.weight: Font.DemiBold
                }
            }

            Rectangle {
                anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                anchors.topMargin: 15
                width: 350; height: 160
                id: listBox
                ToolBar {
                    id: toolBar
                    width: parent.width
                    anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                    RowLayout {
                        anchors.fill: parent
                        property int buttonWidth: 80
                        Item {
                            width: 13
                        }
                        ToolButton {
                            id: tickColHead
                            text: qsTr("Ticker")
                            implicitWidth: parent.buttonWidth
                        }
                        ToolSeparator {}
                        ToolButton {
                            id: intColHead
                            text: qsTr("Interval")
                            implicitWidth: parent.buttonWidth
                        }
                        ToolSeparator {}
                        ToolButton {
                            id: monthColHead
                            text: qsTr("Year-Month")
                            implicitWidth: parent.buttonWidth
                        }
                        Item {
                            width: 13
                        }
                    }
                }
                Component { // Can't set anchor here, set in ListView
                    id: tickDelegate
                    Item {
                        id: tickItem
                        required property string tick
                        width: listBox.width/3; height: 20
                        Text {
                            text: tickItem.tick
                            horizontalAlignment: Text.AlignHCenter
                            width: parent.width
                            clip: true
                        }
                    }
                }
                Component {
                    id: intDelegate
                    Item {
                        id: intItem
                        required property int interval
                        width: listBox.width/3; height: 20
                        Text {
                            text: intItem.interval + ' minutes'
                            horizontalAlignment: Text.AlignHCenter
                            width: parent.width
                            clip: true
                        }
                    }
                }
                Component {
                    id: monthDelegate
                    Item {
                        id: monthItem
                        required property string month
                        width: listBox.width/3; height: 20
                        Text {
                            text: monthItem.month
                            horizontalAlignment: Text.AlignHCenter
                            width: parent.width
                            clip: true
                        }
                    }
                }
                // Set 1st col on left edge of box, 2nd col on right edge of 1st col, 3rd on right edge of 2nd
                ListView {
                    id: tickView
                    anchors.top: toolBar.bottom; anchors.bottom: parent.bottom
                    anchors.left: parent.left // Set on left edge (start)
                    
                    width: parent.width/3

                    model: tickList
                    delegate: tickDelegate
                    highlight: Rectangle { color: "lightsteelblue" }
                    focus: true // TODO: MAKE THIS TRUE ONCLICK, FALSE OFFCLICK
                    clip: true
                }
                ListView {
                    id: intView
                    anchors.top: toolBar.bottom; anchors.bottom: parent.bottom
                    anchors.left: tickView.right // Set on left edge
                    width: parent.width/3

                    model: intList
                    delegate: intDelegate
                    highlight: Rectangle { color: "lightsteelblue" }
                    focus: true // TODO: MAKE THIS TRUE ONCLICK, FALSE OFFCLICK
                    clip: true
                }
                ListView {
                    id: monthView
                    anchors.top: toolBar.bottom; anchors.bottom: parent.bottom
                    anchors.left: intView.right // Set on left edge
                    anchors.right: parent.right // Set right edge (end)

                    model: monthList
                    delegate: monthDelegate
                    highlight: Rectangle { color: "lightsteelblue" }
                    focus: true // TODO: MAKE THIS TRUE ONCLICK, FALSE OFFCLICK
                    clip: true
                }

                ListModel {
                    id: tickList
                    ListElement{ tick: 'NVDA' }
                    ListElement{ tick: 'KO' }
                    ListElement{ tick: 'MSFT' }

                }
                ListModel {
                    id: intList
                    ListElement{ interval:1 }
                    ListElement{ interval:5 }
                    ListElement{ interval:15 }
                    ListElement{ interval:30 }
                    ListElement{ interval:60 }
                }
                ListModel {
                    id: monthList
                    ListElement{ month: '2022-02' }
                }

            }

            Row {
                anchors.bottom: inputRow.top; anchors.left: parent.left
                anchors.bottomMargin: 5
                spacing: 5
                Item { height:1; width: 23 } // Spacer item
                Text { text: qsTr("Ticker:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
                Item { height:1; width: 73 } // Spacer item
                Text { text: qsTr("Interval:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
                Item { height:1; width: 65 } // Spacer item
                Text { text: qsTr("Year-Month:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
            }
            Row {
                id: inputRow
                anchors.bottom: botRow.top; anchors.horizontalCenter: parent.horizontalCenter
                anchors.bottomMargin: 10

                // Text field for ticker
                TextField {
                    id: tickerBox
                    placeholderText: qsTr("SYMB")
                    width: 70 //Math.max(boxBot.width * 0.18, 210) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                }
                RoundButton {
                    text: qsTr("+")
                    onClicked: {
                        
                    }
                    width: 30
                    height: width
                    radius: width/4
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }
                
                Item { height:1; width: 25 } // Spacer item

                // Combo box for selecting interval
                ComboBox {
                    id: intervalList
                    model: [1, 5, 15, 30, 60]
                    width: 60
                    height: 30
                    currentIndex: 0
                    rightPadding: 30
                    leftPadding: -5
                    anchors.verticalCenter: parent.verticalCenter
                    // TODO: ADD OPERATION HERE
                }
                RoundButton {
                    text: qsTr("+")
                    onClicked: {
                        
                    }
                    width: 30
                    height: width
                    radius: width/4
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }

                Item { height:1; width: 25 } // Spacer item

                // Text field for year-month
                TextField {
                    placeholderText: qsTr("YYYY-MM")
                    width: 90 //Math.max(boxBot.width * 0.18, 210) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                }
                RoundButton {
                    text: qsTr("+")
                    onClicked: {
                        
                    }
                    width: 30
                    height: width
                    radius: width/4
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }
            }

            Row {
                id: botRow
                anchors.bottom: boxBot.bottom; anchors.horizontalCenter: parent.horizontalCenter
                anchors.bottomMargin: 10
                spacing: 5

                Button {
                    text: qsTr("Download Intraday")
                    onClicked: {
                        backend._loadData_fromcsv('MSFT', 30, '2022-01', true, true)
                    }
                    width: Math.max(boxBot.width * 0.15, 120) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                    
                }
                Item { height:1; width: 15 } // Spacer item
                TextField {
                    placeholderText: qsTr("API Key: (ex. 59SCKC41X1L8V3N3)")
                    width: Math.max(boxBot.width * 0.18, 210) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                }
            }
        }
    }
}