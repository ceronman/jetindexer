import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes

class Document(
    val path: Path,
    var attributes: BasicFileAttributes,
    val occurrences: Map<String, List<Int>>
)