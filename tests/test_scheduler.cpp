#include "dag_scheduler.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

void test_empty_graph() {
    dag::Dag g;
    dag::Scheduler scheduler(2, false);
    const auto report = scheduler.execute(g);

    assert(report.total_tasks == 0);
    assert(report.succeeded_tasks == 0);
    assert(report.failed_tasks == 0);
    assert(report.events.empty());
}

void test_single_task() {
    dag::Dag g;
    std::atomic<int> counter{0};
    g.add_task({1, [&counter] { ++counter; }});

    dag::Scheduler scheduler(1, false);
    const auto report = scheduler.execute(g);

    assert(counter == 1);
    assert(report.total_tasks == 1);
    assert(report.succeeded_tasks == 1);
    assert(report.failed_tasks == 0);
}

void test_self_loop_forbidden() {
    dag::Dag g;
    g.add_task({1, [] {}});

    bool raised = false;
    try {
        g.add_dependency(1, 1);
    } catch (const std::invalid_argument&) {
        raised = true;
    }

    assert(raised);
}

void test_duplicate_dependency_forbidden() {
    dag::Dag g;
    g.add_task({1, [] {}});
    g.add_task({2, [] {}});
    g.add_dependency(1, 2);

    bool raised = false;
    try {
        g.add_dependency(1, 2);
    } catch (const std::invalid_argument&) {
        raised = true;
    }

    assert(raised);
}

void test_disconnected_dag() {
    dag::Dag g;
    std::atomic<int> executed{0};

    g.add_task({1, [&executed] { ++executed; }});
    g.add_task({2, [&executed] { ++executed; }});
    g.add_task({3, [&executed] { ++executed; }});
    g.add_task({4, [&executed] { ++executed; }});
    g.add_dependency(1, 2);
    g.add_dependency(3, 4);

    dag::Scheduler scheduler(2, false);
    const auto report = scheduler.execute(g);

    assert(executed == 4);
    assert(report.succeeded_tasks == 4);
}

void test_dependency_not_executed_earlier() {
    dag::Dag g;
    std::vector<int> sequence;
    std::mutex seq_mutex;

    g.add_task({1, [&] {
                    std::lock_guard<std::mutex> lock(seq_mutex);
                    sequence.push_back(1);
                }});
    g.add_task({2, [&] {
                    std::lock_guard<std::mutex> lock(seq_mutex);
                    sequence.push_back(2);
                }});
    g.add_dependency(1, 2);

    dag::Scheduler scheduler(2, false);
    (void)scheduler.execute(g);

    assert(sequence.size() == 2);
    auto pos = [&](int id) {
        return std::find(sequence.begin(), sequence.end(), id) - sequence.begin();
    };
    assert(pos(1) < pos(2));
}

void test_execution_report_content() {
    dag::Dag g;
    g.add_task({1, [] { std::this_thread::sleep_for(std::chrono::milliseconds(2)); }});
    g.add_task({2, [] { std::this_thread::sleep_for(std::chrono::milliseconds(2)); }});
    g.add_dependency(1, 2);

    dag::Scheduler scheduler(2, false);
    const auto report = scheduler.execute(g);

    assert(report.total_tasks == 2);
    assert(report.succeeded_tasks == 2);
    assert(report.failed_tasks == 0);
    assert(report.total_duration_ms >= 0);
    assert(!report.events.empty());
    assert(report.ok());
}

void test_exception_handling() {
    dag::Dag g;
    g.add_task({1, [] { throw std::runtime_error("task failure"); }});
    g.add_task({2, [] {}});
    g.add_dependency(1, 2);

    dag::Scheduler scheduler(2, false);
    bool raised = false;
    try {
        (void)scheduler.execute(g);
    } catch (const std::runtime_error&) {
        raised = true;
    }

    assert(raised);
}

void test_file_loader() {
    const auto g = dag::load_dag_from_file(std::filesystem::path("../docs/sample_graph.txt"), [](int) {});
    assert(g.size() == 5);
    const auto order = g.topological_sort();
    assert(order.size() == 5);
}

}  // namespace

int main() {
    test_empty_graph();
    test_single_task();
    test_self_loop_forbidden();
    test_duplicate_dependency_forbidden();
    test_disconnected_dag();
    test_dependency_not_executed_earlier();
    test_execution_report_content();
    test_exception_handling();
    test_file_loader();

    std::cout << "All tests passed\n";
    return 0;
}
