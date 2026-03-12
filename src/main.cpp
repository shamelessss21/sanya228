#include "dag_scheduler.hpp"

#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void simulated_task(int id, int ms) {
    std::ostringstream out;
    out << "[START] task=" << id << " thread=" << std::this_thread::get_id() << '\n';
    std::cout << out.str();

    std::this_thread::sleep_for(std::chrono::milliseconds(ms));

    std::cout << "[DONE ] task=" << id << '\n';
}

dag::Dag build_branched_case() {
    dag::Dag g;
    g.add_task({1, [] { simulated_task(1, 120); }});
    g.add_task({2, [] { simulated_task(2, 180); }});
    g.add_task({3, [] { simulated_task(3, 220); }});
    g.add_task({4, [] { simulated_task(4, 160); }});
    g.add_task({5, [] { simulated_task(5, 140); }});
    g.add_task({6, [] { simulated_task(6, 100); }});

    g.add_dependency(1, 2);
    g.add_dependency(1, 3);
    g.add_dependency(2, 4);
    g.add_dependency(3, 5);
    g.add_dependency(4, 6);
    g.add_dependency(5, 6);
    return g;
}

dag::Dag build_mixed_case() {
    dag::Dag g;
    g.add_task({1, [] { simulated_task(1, 100); }});
    g.add_task({2, [] { simulated_task(2, 100); }});
    g.add_task({3, [] { simulated_task(3, 100); }});
    g.add_task({4, [] { simulated_task(4, 120); }});
    g.add_task({5, [] { simulated_task(5, 90); }});

    g.add_dependency(1, 4);
    g.add_dependency(2, 4);
    g.add_dependency(3, 5);
    g.add_dependency(4, 5);
    return g;
}

void run_case(const std::string& title, const dag::Dag& g, std::size_t threads) {
    std::cout << "\n========================================\n";
    std::cout << title << '\n';
    std::cout << "========================================\n";

    dag::Scheduler scheduler(threads);

    const auto start = std::chrono::steady_clock::now();
    scheduler.execute(g);
    const auto finish = std::chrono::steady_clock::now();

    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start);
    std::cout << "Итоговое время: " << ms.count() << " мс\n";
}

void cycle_test() {
    std::cout << "\n========================================\n";
    std::cout << "Тест: обнаружение цикла\n";
    std::cout << "========================================\n";

    dag::Dag g;
    g.add_task({1, [] { simulated_task(1, 50); }});
    g.add_task({2, [] { simulated_task(2, 50); }});
    g.add_dependency(1, 2);
    g.add_dependency(2, 1);

    try {
        auto order = g.topological_sort();
        (void)order;
        throw std::runtime_error("Цикл не был обнаружен");
    } catch (const std::runtime_error& e) {
        std::cout << "Ожидаемый результат: " << e.what() << '\n';
    }
}

}  // namespace

int main() {
    try {
        std::cout << "Система планирования задач на основе DAG (C++)\n";

        run_case("Сценарий 1: ветвящийся DAG", build_branched_case(), 4);
        run_case("Сценарий 2: смешанный DAG", build_mixed_case(), 4);
        cycle_test();

        std::cout << "\nВсе сценарии завершены успешно.\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Критическая ошибка: " << e.what() << '\n';
        return 1;
    }
}
