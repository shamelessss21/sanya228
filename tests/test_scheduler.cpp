#include "dag_scheduler.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

void test_topological_sort_respects_dependencies() {
    dag::Dag g;
    g.add_task({1, [] {}});
    g.add_task({2, [] {}});
    g.add_task({3, [] {}});
    g.add_task({4, [] {}});

    g.add_dependency(1, 2);
    g.add_dependency(1, 3);
    g.add_dependency(2, 4);
    g.add_dependency(3, 4);

    const auto order = g.topological_sort();

    auto pos = [&](int id) {
        for (std::size_t i = 0; i < order.size(); ++i) {
            if (order[i] == id) {
                return i;
            }
        }
        throw std::runtime_error("id not found in order");
    };

    assert(order.size() == 4);
    assert(pos(1) < pos(2));
    assert(pos(1) < pos(3));
    assert(pos(2) < pos(4));
    assert(pos(3) < pos(4));
}

void test_cycle_detection() {
    dag::Dag g;
    g.add_task({1, [] {}});
    g.add_task({2, [] {}});
    g.add_dependency(1, 2);
    g.add_dependency(2, 1);

    bool raised = false;
    try {
        (void)g.topological_sort();
    } catch (const std::runtime_error&) {
        raised = true;
    }

    assert(raised);
}

void test_scheduler_executes_all_tasks_once() {
    dag::Dag g;

    std::atomic<int> counter{0};
    for (int i = 1; i <= 8; ++i) {
        g.add_task({i, [&counter] {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            ++counter;
        }});
    }

    g.add_dependency(1, 5);
    g.add_dependency(2, 5);
    g.add_dependency(3, 6);
    g.add_dependency(4, 6);
    g.add_dependency(5, 7);
    g.add_dependency(6, 8);

    dag::Scheduler scheduler(4);
    scheduler.execute(g);

    assert(counter == 8);
}

void test_scheduler_propagates_exception() {
    dag::Dag g;
    g.add_task({1, [] { throw std::runtime_error("task failure"); }});

    dag::Scheduler scheduler(2);
    bool raised = false;
    try {
        scheduler.execute(g);
    } catch (const std::runtime_error&) {
        raised = true;
    }

    assert(raised);
}

}  // namespace

int main() {
    test_topological_sort_respects_dependencies();
    test_cycle_detection();
    test_scheduler_executes_all_tasks_once();
    test_scheduler_propagates_exception();

    std::cout << "All tests passed\n";
    return 0;
}
