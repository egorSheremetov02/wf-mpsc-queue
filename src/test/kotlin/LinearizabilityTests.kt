import com.esh.jbr.queue.WaitFreeMPSCQueue
import com.esh.jbr.queue.SimpleMPMCQueue
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import java.util.*
import kotlin.test.Test

class TestSimpleMPMCQueue {
    private val q = SimpleMPMCQueue<Int>()

    // Operations on the Counter
    @Operation
    fun enqueue(el: Int) = q.enqueue(el)

    @Operation(nonParallelGroup = "Consumer")
    fun dequeue() = q.dequeue()

    @Test
    fun stressTest() = StressOptions().check(this::class)

    @Test
    fun modelCheckingTest() = ModelCheckingOptions().check(this::class)
}

class TestWaitFreeMPSCQueue {
    private val q = WaitFreeMPSCQueue<Int>(0)

    // Operations on the Counter
    @Operation
    fun enqueue(el: Int) = q.enqueue(el)

    @Operation(nonParallelGroup = "Consumer")
    fun dequeue() = q.dequeue()

    @Test
    fun stressTest() = StressOptions().check(this::class)

    @Test
    fun modelCheckingTest() = ModelCheckingOptions().check(this::class)
}
