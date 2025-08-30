from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QTextEdit, QPushButton
from PySide6.QtQml import QQmlApplicationEngine

# Import backend interfacer
from .backendInterfacer import Backend

# Only needed for access to command line arguments
import sys

# You need one (and only one) QApplication instance per application.
# Pass in sys.argv to allow command line arguments for your app.
# If you know you won't use command line arguments QApplication([]) works too.
app = QApplication(sys.argv)

# Create a Qt widget, which will be our window.
#window = QWidget()
#window.show()  # IMPORTANT!!!!! Windows are hidden by default.

engine = QQmlApplicationEngine()

# Initialise backend here
backend = Backend()
engine.rootContext().setContextProperty("backend", backend)

# Expose data tabs
engine.rootContext().setContextProperty("dfModel", backend.dfModel)
engine.rootContext().setContextProperty("tabModel", backend.tabModel)

engine.load("arcanequant/fend/SimpleView.qml")

screen = app.primaryScreen()
size = screen.availableGeometry()
print(f"Screen width: {size.width()}, height: {size.height()}")



# Start the event loop.
app.exec()

# Your application won't reach here until you exit and the event
# loop has stopped.


class CLIWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CLI Inside GUI")
        self.setLayout(QVBoxLayout())

        self.output = QTextEdit()
        self.output.setReadOnly(True)
        self.input = QTextEdit()
        self.run_button = QPushButton("Run")

        self.run_button.clicked.connect(self.run_command)

        self.layout().addWidget(self.output)
        self.layout().addWidget(self.input)
        self.layout().addWidget(self.run_button)

    def run_command(self):
        cmd = self.input.toPlainText().strip()
        self.output.append(f"> {cmd}")
        # simulate command execution
        if cmd == "hello":
            self.output.append("Hello, user!")
        elif cmd == "clear":
            self.output.clear()
        else:
            self.output.append("Unknown command.")

#w = CLIWidget()
#w.show()
#app.exec()