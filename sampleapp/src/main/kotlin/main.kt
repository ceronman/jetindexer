import com.ceronman.jetindexer.DefaultIndexingFilter
import com.ceronman.jetindexer.JetIndexer
import com.ceronman.jetindexer.TrigramSubstringQueryResolver
import com.ceronman.jetindexer.TrigramTokenizer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.awt.BorderLayout
import java.awt.Dimension
import javax.swing.*
import javax.swing.event.DocumentEvent
import javax.swing.event.DocumentListener

fun main() {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO")
    lateinit var indexer: JetIndexer

    val progressBar = JProgressBar()
    progressBar.isVisible = false

    val logArea = JTextArea()
    logArea.text = "Please select a directory to watch"
    logArea.isEditable = false

    val inputBox = JTextField()
    inputBox.text = "input query"
    inputBox.isEnabled = false

    fun search() {
        GlobalScope.launch(Dispatchers.Default) {
            val term = inputBox.text
            if (term.isEmpty()) {
                return@launch
            }
            val results = indexer.query(term)
            val resultText = StringBuilder()
            for (result in results) {
                resultText.append("${result.path}:${result.position}\n")
            }
            logArea.text = resultText.toString()
        }
    }

    inputBox.document.addDocumentListener(object : DocumentListener {
        override fun insertUpdate(e: DocumentEvent?) = search()
        override fun removeUpdate(e: DocumentEvent?) = search()
        override fun changedUpdate(e: DocumentEvent?) = search()
    })


    val selectDirButton = JButton("Select directory to watch")
    selectDirButton.addActionListener {
        val fileChooser = JFileChooser()
        fileChooser.fileSelectionMode = JFileChooser.DIRECTORIES_ONLY
        if (fileChooser.showOpenDialog(selectDirButton) == JFileChooser.APPROVE_OPTION) {
            indexer = JetIndexer(
                TrigramTokenizer(),
                TrigramSubstringQueryResolver(),
                DefaultIndexingFilter(),
                listOf(fileChooser.selectedFile.toPath())
            )
            GlobalScope.launch(Dispatchers.Default) {
                progressBar.isVisible = true
                indexer.index { progress -> progressBar.value = progress }
                progressBar.isVisible = false
                inputBox.text = ""
                inputBox.isEnabled = true
                inputBox.isEditable = true
                logArea.text = "^ Type something in the box above!"
                indexer.watch {
                    search()
                }
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