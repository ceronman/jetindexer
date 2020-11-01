import com.ceronman.jetindexer.JetIndexer
import com.ceronman.jetindexer.WhiteSpaceTokenizer
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.awt.BorderLayout
import java.awt.Dimension
import java.lang.StringBuilder
import javax.swing.*
import javax.swing.event.DocumentEvent
import javax.swing.event.DocumentListener

fun main(args: Array<String>) {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO")
    lateinit var indexer: JetIndexer

    val progressBar = JProgressBar()

    val logArea = JTextArea()
    logArea.text = "Please select a directory to watch"
    logArea.isEditable = false

    val inputBox = JTextField()
    inputBox.text = "input query"
    inputBox.isEnabled = false

    inputBox.document.addDocumentListener(object: DocumentListener {
        override fun insertUpdate(e: DocumentEvent?) {
            search()
        }

        override fun removeUpdate(e: DocumentEvent?) {
            search()
        }

        override fun changedUpdate(e: DocumentEvent?) {
            search()
        }

        fun search() {
            val term = inputBox.text
            if (term.isEmpty()) {
                return
            }
            val results = indexer.query(term)
            val resultText = StringBuilder()
            for (result in results) {
                resultText.append("${result.path}:${result.position}\n")
            }
            logArea.text = resultText.toString()
        }
    })


    val selectDirButton = JButton("Select directory to watch")
    selectDirButton.addActionListener {
        val fileChooser = JFileChooser()
        fileChooser.fileSelectionMode = JFileChooser.DIRECTORIES_ONLY
        if (fileChooser.showOpenDialog(selectDirButton) == JFileChooser.APPROVE_OPTION) {
            println("Selected ${fileChooser.selectedFile}")
            indexer = JetIndexer(WhiteSpaceTokenizer(), listOf(fileChooser.selectedFile.toPath()))
            GlobalScope.launch { indexer.index() }
            GlobalScope.launch {
                for (p in indexer.indexingProgress) {
                    progressBar.value = (p * 100.0).toInt()
                }
                inputBox.text = ""
                inputBox.isEnabled = true
                inputBox.isEditable = true
                logArea.text = "^ Type something in the box above!"
            }
            selectDirButton.isEnabled = false
        }
    }

    val searchPanel = JPanel()
    searchPanel.layout = BorderLayout()
    searchPanel.add(selectDirButton, BorderLayout.PAGE_START)
    searchPanel.add(inputBox, BorderLayout.PAGE_END)

    val panel = JPanel()
    panel.layout = BorderLayout()
    panel.add(searchPanel, BorderLayout.PAGE_START)
    panel.add(progressBar, BorderLayout.AFTER_LAST_LINE)
    panel.add(JScrollPane(logArea), BorderLayout.CENTER)

    val frame = JFrame("JetIndexer sample applicatoin")
    frame.contentPane.add(panel, BorderLayout.CENTER)
    frame.defaultCloseOperation = JFrame.EXIT_ON_CLOSE
    frame.size = Dimension(1000, 1000)
    frame.setLocationRelativeTo(null)
    frame.isVisible = true

}