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
        anchors.topMargin: 20
        Rectangle {
            id: boxTop
            Layout.preferredHeight: parent.height/7
            Layout.minimumHeight: 75
            Layout.fillWidth: true
            color: itemColour
            radius: 8
            border.color: subItemBorderColour
            Layout.margins: 5

            

            Rectangle{
                anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                color: itemColour
                height: title.height/2
                width: saveloadHeader.width*1.1
                Text {
                    id: saveloadHeader
                    anchors.verticalCenter: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                    text: qsTr("Save / Load Options")
                    font.pixelSize: 18
                    font.weight: Font.DemiBold
                }
            }
            Row {
                anchors.bottom: saveloadOptions.bottom; anchors.horizontalCenter: parent.horizontalCenter
                anchors.bottomMargin: 35
                Text { text: qsTr("Downloaded Data Save setting:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}

                Item { height:1; width: 30 } // Spacer item
                
                Text { text: qsTr("Extract Data Load setting:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
            }

            Row {
                id: saveloadOptions
                anchors.bottom: parent.bottom; anchors.left: parent.left
                anchors.leftMargin: 55
                anchors.bottomMargin: 15
                spacing: 5
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

                Item { height:1; width: 75 } // Spacer item
                
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
                anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                color: itemColour
                height: title.height/2
                width: udeHeader.width*1.1
                Text {
                    id: udeHeader
                    anchors.verticalCenter: parent.top; anchors.horizontalCenter: parent.horizontalCenter
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

                // Data availability Indicator
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
                    // Tooltip settings
                    ToolTip {
                        parent: parent
                        visible: parent.hovered
                        delay: 1000
                        dim: true
                        text: qsTr("Extracts data for a single ticker, in a single interval for one month-period.")

                        x: (parent.width-width)/2
                        y: parent.height
                        width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
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
                    // Tooltip settings
                    ToolTip {
                        parent: parent
                        visible: parent.hovered
                        delay: 1000
                        dim: true
                        text: qsTr("Includes meta-data descriptions of the unit data (as a separate dataset).")

                        x: (parent.width-width)/2
                        y: parent.height
                        width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
                    }
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

            Rectangle {
                anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                color: itemColour
                height: title.height/2
                width: dedHeader.width*1.1
                Text {
                    id: dedHeader
                    anchors.verticalCenter: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                    text: qsTr("Data Extractor / Downloader")
                    font.pixelSize: 18
                    font.weight: Font.DemiBold
                }
            }

            Rectangle {
                id: listBox
                anchors.top: parent.top; anchors.left: parent.left
                anchors.topMargin: 20
                anchors.leftMargin: 25
                width: 190; height: 165
                border.color: "blue"
                z:3

                // To control Esc use for both lists
                Keys.onPressed: function(event) {
                    if (event.key === Qt.Key_Escape) {
                        intView.forceActiveFocus(false)  // Remove keyboard focus
                        intView.focus = false
                        tickView.forceActiveFocus(false)
                        tickView.focus = false
                        event.accepted = true // Stop further propagation (to other events)
                    }
                }

                ToolBar {
                    id: toolBar
                    width: parent.width-2
                    anchors.top: parent.top; anchors.horizontalCenter: parent.horizontalCenter
                    anchors.topMargin: 1 // To show border
                    RowLayout {
                        anchors.fill: parent
                        property int buttonWidth: 60
                        Item {
                            width: 13
                        }
                        ToolButton {
                            id: tickColHead
                            text: qsTr("Tickers")
                            implicitWidth: parent.buttonWidth
                        }
                        ToolSeparator {}
                        ToolButton {
                            id: intColHead
                            text: qsTr("Intervals")
                            implicitWidth: parent.buttonWidth
                        }
                        Item {
                            width: 13
                        }
                    }
                }

                // Set 1st col on left edge of box, 2nd col on right edge of 1st col, 3rd on right edge of 2nd
                ListView {
                    id: tickView
                    anchors.top: toolBar.bottom; anchors.bottom: parent.bottom
                    anchors.bottomMargin: 1; anchors.leftMargin: 1 // To show border
                    anchors.left: parent.left // Set on left edge (start)
                    
                    width: parent.width/2 -1

                    model: backend.tgtTickers
                    delegate: Item {
                        width: listBox.width/2; height: 20
                        Text {
                            text: modelData
                            horizontalAlignment: Text.AlignHCenter
                            width: parent.width
                            clip: true
                        }

                        // Click settings
                        MouseArea {
                            anchors.fill: parent
                            onClicked: {
                                tickView.currentIndex = index
                                tickView.focus = true
                            }
                        }
                    }
                    highlight: Rectangle { color: "lightsteelblue" }
                    clip: true

                    currentIndex: -1
                    onModelChanged: {
                        currentIndex = -1  // De-select any item when new added (avoid bugs)
                    }

                    // Click/Keyboard settings
                    focus: false
                    onFocusChanged: {
                        if (!focus) { currentIndex = -1 }
                    }
                    Keys.onDownPressed: currentIndex = Math.min(currentIndex + 1, count - 1)
                    Keys.onUpPressed: currentIndex = Math.max(currentIndex - 1, 0)
                }
                ListView {
                    id: intView
                    anchors.top: toolBar.bottom; anchors.bottom: parent.bottom
                    anchors.left: tickView.right // Set on left edge
                    width: parent.width/2 -1
                    model: backend.tgtIntervals
                    anchors.bottomMargin: 1 // To show border
                    
                    delegate: Item {
                        width: listBox.width/2; height: 20
                        Text {
                            text: modelData == 'None' ? modelData : modelData + ' minute'
                            horizontalAlignment: Text.AlignHCenter
                            width: parent.width
                            clip: true
                        }

                        // Click settings
                        MouseArea {
                            anchors.fill: parent
                            onClicked: {
                                intView.currentIndex = index
                                intView.focus = true
                            }
                        }
                    }
                    highlight: Rectangle { color: "lightsteelblue" }
                    clip: true

                    currentIndex: -1
                    onModelChanged: {
                        currentIndex = -1  // De-select any item when new added (avoid bugs)
                    }

                    // Click/Keyboard settings
                    focus: false
                    onFocusChanged: {
                        if (!focus) { currentIndex = -1 }
                    }
                    Keys.onDownPressed: currentIndex = Math.min(currentIndex + 1, count - 1)
                    Keys.onUpPressed: currentIndex = Math.max(currentIndex - 1, 0)
                }
            }

            // Remove parameter (ticker or interval)
            Button {
                anchors.horizontalCenter: listBox.horizontalCenter; anchors.top: listBox.bottom
                anchors.topMargin: 5
                text: qsTr("Remove Parameter")
                focusPolicy: Qt.NoFocus  // Prevents button from taking focus (otherwise list focus doesn't work)

                // Enabled if either interval or ticker list in focus (also that list has to not be empty)
                enabled: {
                    (intView.focus && intView.model[0] !== 'None' ) | (tickView.focus && tickView.model[0] !== 'None')
                }
               
                onClicked: {
                    if (intView.focus) {
                        backend._rmInterval(intView.model[intView.currentIndex])
                        intView.focus = false
                    }
                    if (tickView.focus) {
                        backend._rmTicker(tickView.model[tickView.currentIndex])
                        tickView.focus = false
                    }
                }
                width: Math.max(parent.width * 0.09, 120) //RelativeSize
                height: 30 //Math.max(parent.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                leftPadding: width*0.08 //RelativeSize
                rightPadding: width*0.08 //RelativeSize
            }

            Column {
                anchors.left: listBox.right
                anchors.verticalCenter: listBox.verticalCenter
                anchors.leftMargin: 40
                spacing: 5

                Text { 
                    text: qsTr("Start Year-Month Period:")
                    anchors.horizontalCenter: startYM.horizontalCenter
                }
                // Text field for year-month (start)
                TextField {
                    id: startYM
                    placeholderText: qsTr("YYYY-MM")
                    width: 90 //Math.max(boxBot.width * 0.18, 210) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.04, 30) //RelativeSize
                    // Tooltip settings
                    ToolTip {
                        parent: parent
                        visible: parent.hovered
                        delay: 1000
                        dim: true
                        text: qsTr("Other date formats acceptable but prone to misreading.")

                        x: (parent.width-width)/2
                        y: parent.height
                        width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
                    }
                }

                Item { height:25; width: 1 } // Spacer item

                Text { 
                    text: qsTr("End Year-Month Period:")
                    anchors.horizontalCenter: endYM.horizontalCenter
                }
                // Text field for year-month (end)
                TextField {
                    id: endYM
                    placeholderText: qsTr("YYYY-MM")
                    width: 90 //Math.max(boxBot.width * 0.18, 210) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.04, 30) //RelativeSize
                    // Tooltip settings
                    ToolTip {
                        parent: parent
                        visible: parent.hovered
                        delay: 1000
                        dim: true
                        text: qsTr("Other date formats acceptable but prone to misreading.")

                        x: (parent.width-width)/2
                        y: parent.height
                        width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
                    }
                }
            }

            Row {
                id: inputRowText
                anchors.bottom: inputRow.top; anchors.left: parent.left
                anchors.bottomMargin: 5
                spacing: 5
                Item { height:1; width: 23 } // Spacer item
                Text { text: qsTr("Ticker:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
                Item { height:1; width: 63 } // Spacer item
                Text { text: qsTr("Interval:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
                Item { height:1; width: 80 } // Spacer item
                Text { text: qsTr("API Key:"); font.pixelSize: 12; anchors.verticalCenter: parent.verticalCenter}
            }

            Row {
                id: inputRow
                anchors.bottom: botRow.top; anchors.left: parent.left
                anchors.bottomMargin: 15
                
                Item { height:1; width: 10 } // Spacer item
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
                        backend._addTicker(tickerBox.text)
                    }
                    width: 30
                    height: width
                    radius: width/4
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }
                
                Item { height:1; width: 15 } // Spacer item

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
                        backend._addInterval(intervalList.currentValue)
                    }
                    width: 30
                    height: width
                    radius: width/4
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }

                Item { height:1; width: 15 } // Spacer item

                TextField {
                    id: apiKey
                    placeholderText: qsTr("59SCKC41X1L8V3N3")
                    width: Math.max(boxBot.width * 0.15, 135) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                    // Tooltip settings
                    ToolTip {
                        parent: parent
                        visible: parent.hovered
                        delay: 1000
                        dim: true
                        text: qsTr("Alphavantage API key needed only for downloading intraday data.")

                        x: (parent.width-width)/2
                        y: parent.height
                        width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
                    }
                }
            }

            Row {
                id: botRow
                anchors.bottom: parent.bottom; anchors.horizontalCenter: parent.horizontalCenter
                anchors.bottomMargin: 15
                spacing: 5

                // Download data
                Button {
                    text: qsTr("Download Intraday")
                    onClicked: {
                        
                    }
                    // New makes Date object (otherwise a string representation), if valid dates (and interval/ticker provided) enable button
                    enabled: {
                        intView.model[0] !== 'None' && tickView.model[0] !== 'None' &&
                        !isNaN(new Date(startYM.text).getTime()) && !isNaN(new Date(endYM.text).getTime()) &&
                        // Check save setting valid
                        dmController.canSaveLoad(saveops.currentValue) &&
                        // Make sure API Key not empty
                        apiKey.text
                    }

                    // Tooltip settings
                    ToolTip {
                        parent: parent
                        visible: parent.hovered
                        delay: 1000
                        dim: true
                        text: qsTr("Download set of unit data from Alphavantage API.")

                        x: (parent.width-width)/2
                        y: parent.height
                        width: Math.min(contentWidth + 2*horizontalPadding, parent.width*2)
                    }
                    
                    width: Math.max(boxBot.width * 0.15, 120) //RelativeSize
                    height: 30 //Math.max(boxBot.height * 0.15, 30) //RelativeSize //TODO: ADD OPERATION
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }

                Item { height:1; width: 15 } // Spacer item

                // Extract data (arbitrary ticker, months, intervals)
                Button {
                    text: qsTr("Extract Data")
                    // New makes Date object (otherwise a string representation), if valid dates (and interval/ticker provided) enable button
                    enabled: {
                        intView.model[0] !== 'None' && tickView.model[0] !== 'None' &&
                        !isNaN(new Date(startYM.text).getTime()) && !isNaN(new Date(endYM.text).getTime()) &&
                        // Check load setting valid
                        dmController.canSaveLoad(loadops.currentValue)
                    }
                    
                    onClicked: {
                        let requestSQL = null
                        if (loadops.currentValue == 'Direct') {
                            requestSQL = false
                        } else if (loadops.currentValue == 'Database') {
                            requestSQL = true
                        }
                        backend._ExtractData(startYM.text, endYM.text, requestSQL)
                    }
                    width: Math.max(parent.width * 0.09, 120) //RelativeSize
                    height: 30 //Math.max(parent.height * 0.04, 30) //RelativeSize //TODO: ADD OPERATION
                    leftPadding: width*0.08 //RelativeSize
                    rightPadding: width*0.08 //RelativeSize
                }
            }
        }
    }
}