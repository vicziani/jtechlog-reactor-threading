package jtechlog;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
class ReactorThreadingTest {

    List<Employee> employees = List.of(
            new Employee("John Doe", 1970),
            new Employee("Jack Doe", 1985),
            new Employee("Jane Doe", 1990)
    );

    @Test
    void onMainThread() {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                ;

        log.info("Pipeline is ready");

        StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete()
        ;
    }

    @Test
    void onAnotherThread() throws InterruptedException {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                ;

        log.info("Pipeline is ready");

        var anotherThread = new Thread(() -> StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete())
        ;
        anotherThread.start();
        anotherThread.join();
    }

    @Test
    void delaySequence() {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .delaySequence(Duration.ofMillis(10))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                ;

        log.info("Pipeline is ready");

        StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete()
        ;
    }

    @Test
    void publishOn() {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .publishOn(Schedulers.newParallel("p1"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                ;

        log.info("Pipeline is ready");

        StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete()
        ;
    }

    @Test
    void publishOnTwice() {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .publishOn(Schedulers.newParallel("p1"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .publishOn(Schedulers.newParallel("p2"))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                ;

        log.info("Pipeline is ready");

        StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete()
        ;
    }

    @Test
    void subscribeOn() {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                .subscribeOn(Schedulers.newParallel("s1"))
                ;

        log.info("Pipeline is ready");

        StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete()
        ;
    }

    @Test
    void subscribeOnTwice() {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .subscribeOn(Schedulers.newParallel("s1"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                .subscribeOn(Schedulers.newParallel("s2"))
                ;

        log.info("Pipeline is ready");

        StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete()
        ;
    }

    @Test
    void mixed() {
        var upperCaseNames = Flux.fromIterable(employees)
                .filter(employee -> employee.getYearOfBirth() >= 1980)
                .doOnNext(e -> log.info("filter"))
                .publishOn(Schedulers.newParallel("p1"))
                .map(Employee::getName)
                .doOnNext(e -> log.info("map - getName"))
                .publishOn(Schedulers.newParallel("p2"))
                .map(String::toUpperCase)
                .doOnNext(e -> log.info("map - toUpperCase"))
                .subscribeOn(Schedulers.newParallel("s1"))
                ;

        log.info("Pipeline is ready");

        StepVerifier.create(upperCaseNames)
                .expectNext("JACK DOE")
                .expectNext("JANE DOE")
                .verifyComplete()
        ;
    }

}
