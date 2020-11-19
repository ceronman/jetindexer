// Copyright Manuel Cerón. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import com.ceronman.jetindexer.DefaultIndexingFilter
import com.ceronman.jetindexer.JetIndexer
import com.ceronman.jetindexer.TrigramSubstringQueryResolver
import com.ceronman.jetindexer.TrigramTokenizer
import kotlinx.coroutines.*
import java.awt.BorderLayout
import java.awt.Dimension
import javax.swing.*
import javax.swing.event.DocumentEvent
import javax.swing.event.DocumentListener

fun main() {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO")
    lateinit var indexer: JetIndexer
    var searchJob: Job? = null

    val progressBar = JProgressBar()
    progressBar.isVisible = false

    val logArea = JTextArea()
    logArea.text = "Please select a directory to watch"
    logArea.isEditable = false

    val inputBox = JTextField()
    inputBox.text = "input query"
    inputBox.isEditable = false

    fun search() {
        searchJob?.cancel()
        searchJob = GlobalScope.launch(Dispatchers.Default) {
            val term = inputBox.text
            if (term.isEmpty()) {
                logArea.text = "^ Type something in the box above!"
                return@launch
            }
            logArea.text = ""
            if (term.length < 3) {
                logArea.text = "WARN Queries smaller than 3 characters are not indexed. They require a full scan search\n"
                return@launch
            }
            val results = indexer.query(term)

            for (chunk in results.chunked(100)) {
                if (!isActive) {
                    break
                }
                val resultText = StringBuilder()
                for (result in chunk) {
                    resultText.append("${result.path}:${result.position}\n")
                }
                logArea.text += resultText.toString()
            }
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
                listOf(fileChooser.selectedFile.toPath()),
                TrigramTokenizer(),
                TrigramSubstringQueryResolver(),
                DefaultIndexingFilter(),
            )
            inputBox.isEditable = true
            GlobalScope.launch(Dispatchers.Default) {
                progressBar.isVisible = true
                inputBox.text = ""
                indexer.index { progress -> progressBar.value = progress }
                progressBar.isVisible = false
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

    val frame = JFrame("JetIndexer sample application")
    frame.contentPane.add(panel, BorderLayout.CENTER)
    frame.defaultCloseOperation = JFrame.EXIT_ON_CLOSE
    frame.size = Dimension(1000, 1000)
    frame.setLocationRelativeTo(null)
    frame.isVisible = true

}