#include "dag_scheduler.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

void simulated_task(int id, int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    std::cout << "  user-task " << id << " done in " << ms << "ms\n";
}

dag::Dag build_demo_case() {
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

void print_report(const dag::ExecutionReport& report) {
    std::cout << "\n----- ExecutionReport -----\n";
    std::cout << "Всего задач: " << report.total_tasks << '\n';
    std::cout << "Успешно:     " << report.succeeded_tasks << '\n';
    std::cout << "Провалено:   " << report.failed_tasks << '\n';
    std::cout << "Время:       " << report.total_duration_ms << " мс\n";
    if (!report.error_message.empty()) {
        std::cout << "Ошибка:      " << report.error_message << '\n';
    }
    std::cout << "События (фактический порядок):\n";
    for (const auto& event : report.events) {
        std::cout << "  #" << event.sequence << " [" << event.timestamp_ms << "ms] task=" << event.task_id
                  << " -> " << dag::to_string(event.state);
        if (!event.message.empty()) {
            std::cout << " (" << event.message << ")";
        }
        std::cout << '\n';
    }
}

void run_performance_experiment(const dag::Dag& dag) {
    std::cout << "\n=== Эксперимент производительности (1/2/4 потока) ===\n";

    std::vector<std::size_t> threads{1, 2, 4};
    std::vector<long long> durations;

    for (std::size_t thread_count : threads) {
        dag::Scheduler scheduler(thread_count, true);
        const auto report = scheduler.execute(dag);
        durations.push_back(report.total_duration_ms);
        std::cout << "Потоков: " << thread_count << ", время: " << report.total_duration_ms << " мс\n";
    }

    const double base = static_cast<double>(durations.front());
    std::cout << "\nУскорение относительно 1 потока:\n";
    for (std::size_t i = 0; i < threads.size(); ++i) {
        const double speedup = base / static_cast<double>(durations[i]);
        std::cout << "  " << threads[i] << " потока(ов): x" << speedup << '\n';
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        std::cout << "Система планирования задач на основе DAG (C++20)\n";

        dag::Dag dag_graph;
        if (argc > 1) {
            const std::string input_path = argv[1];
            std::cout << "Загрузка графа из файла: " << input_path << '\n';
            dag_graph = dag::load_dag_from_file(input_path, [](int id) {
                simulated_task(id, 100 + (id % 4) * 20);
            });
        } else {
            dag_graph = build_demo_case();
        }

        dag::Scheduler scheduler(4, true);
        const auto report = scheduler.execute(dag_graph);
        print_report(report);

        run_performance_experiment(dag_graph);

        std::cout << "\nВсе сценарии завершены успешно.\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& e) {
        std::cerr << "Критическая ошибка: " << e.what() << '\n';
        return EXIT_FAILURE;
    }
}
