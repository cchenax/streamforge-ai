package ai.streamforge.processor.drift;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class DriftSignalFunctionTest {

    @ParameterizedTest(name = "count={0} → bucket={1}")
    @CsvSource({
        "1,   1",
        "2,   2-5",
        "5,   2-5",
        "6,   6-10",
        "10,  6-10",
        "11,  11-20",
        "20,  11-20",
        "21,  21-50",
        "50,  21-50",
        "51,  51-100",
        "100, 51-100",
        "101, 100+",
        "999, 100+"
    })
    void bucketAssignment(long count, String expectedBucket) {
        assertThat(DriftSignalFunction.bucket(count)).isEqualTo(expectedBucket);
    }
}
