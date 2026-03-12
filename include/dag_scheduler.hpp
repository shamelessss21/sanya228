#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dag {

enum class TaskState {
    Pending,
    Ready,
    Running,
    Completed,
    Failed
};

inline const char* to_string(TaskState state) {
    switch (state) {
    case TaskState::Pending:
        return "Pending";
    case TaskState::Ready:
        return "Ready";
    case TaskState::Running:
        return "Running";
    case TaskState::Completed:
        return "Completed";
    case TaskState::Failed:
        return "Failed";
    }
    return "Unknown";
}

struct Task {
    int id{};
    std::function<void()> action;
};

struct TaskEvent {
    std::size_t sequence{};
    int task_id{};
    TaskState state{TaskState::Pending};
    long long timestamp_ms{};
    std::string message;
};

struct ExecutionReport {
    std::size_t total_tasks{};
    std::size_t succeeded_tasks{};
    std::size_t failed_tasks{};
    long long total_duration_ms{};
    std::vector<TaskEvent> events;
    std::string error_message;
    std::unordered_map<int, TaskState> final_states;

    [[nodiscard]] bool ok() const {
        return failed_tasks == 0 && error_message.empty();
    }
};

class Dag {
public:
    void add_task(Task task) {
        if (!task.action) {
            throw std::invalid_argument("Для задачи не задана функция выполнения");
        }
        if (tasks_.count(task.id) > 0) {
            throw std::invalid_argument("Задача с id=" + std::to_string(task.id) + " уже существует");
        }

        const int id = task.id;
        tasks_.emplace(id, std::move(task));
        adjacency_[id];
        indegree_.try_emplace(id, 0);
    }

    void add_dependency(int from, int to) {
        ensure_exists(from);
        ensure_exists(to);

        if (from == to) {
            throw std::invalid_argument("Self-loop запрещен: задача " + std::to_string(from) + " не может зависеть от самой себя");
        }

        const auto edge = edge_key(from, to);
        if (!edges_.insert(edge).second) {
            throw std::invalid_argument("Дублирующая зависимость запрещена: " + std::to_string(from) + " -> " + std::to_string(to));
        }

        adjacency_[from].push_back(to);
        ++indegree_[to];
    }

    void validate() const {
        if (tasks_.empty()) {
            return;
        }

        for (const auto& [id, _] : tasks_) {
            if (adjacency_.count(id) == 0 || indegree_.count(id) == 0) {
                throw std::runtime_error("Граф поврежден: отсутствуют служебные структуры для задачи id=" + std::to_string(id));
            }
        }

        (void)topological_sort();
    }

    [[nodiscard]] std::vector<int> topological_sort() const {
        std::unordered_map<int, int> current_indegree = indegree_;
        std::queue<int> q;

        for (const auto& [id, _] : tasks_) {
            if (current_indegree[id] == 0) {
                q.push(id);
            }
        }

        std::vector<int> order;
        order.reserve(tasks_.size());

        while (!q.empty()) {
            int node = q.front();
            q.pop();
            order.push_back(node);

            auto it = adjacency_.find(node);
            if (it == adjacency_.end()) {
                continue;
            }

            for (int next : it->second) {
                if (--current_indegree[next] == 0) {
                    q.push(next);
                }
            }
        }

        if (order.size() != tasks_.size()) {
            throw std::runtime_error("Обнаружен цикл в графе зависимостей: топологическая сортировка невозможна");
        }

        return order;
    }

    [[nodiscard]] const Task& task(int id) const {
        auto it = tasks_.find(id);
        if (it == tasks_.end()) {
            throw std::out_of_range("Задача не найдена: id=" + std::to_string(id));
        }
        return it->second;
    }

    [[nodiscard]] const std::vector<int>& dependents(int id) const {
        auto it = adjacency_.find(id);
        if (it == adjacency_.end()) {
            throw std::out_of_range("Задача не найдена: id=" + std::to_string(id));
        }
        return it->second;
    }

    [[nodiscard]] std::unordered_map<int, int> indegree_map() const {
        return indegree_;
    }

    [[nodiscard]] std::vector<int> task_ids() const {
        std::vector<int> ids;
        ids.reserve(tasks_.size());
        for (const auto& [id, _] : tasks_) {
            ids.push_back(id);
        }
        return ids;
    }

    [[nodiscard]] std::size_t size() const {
        return tasks_.size();
    }

private:
    static std::uint64_t edge_key(int from, int to) {
        return (static_cast<std::uint64_t>(static_cast<std::uint32_t>(from)) << 32U) |
               static_cast<std::uint32_t>(to);
    }

    void ensure_exists(int id) const {
        if (tasks_.count(id) == 0) {
            throw std::invalid_argument("Попытка создать зависимость для несуществующей задачи id=" + std::to_string(id));
        }
    }

    std::unordered_map<int, Task> tasks_;
    std::unordered_map<int, std::vector<int>> adjacency_;
    std::unordered_map<int, int> indegree_;
    std::unordered_set<std::uint64_t> edges_;
};

class Scheduler {
public:
    explicit Scheduler(std::size_t thread_count, bool verbose_logging = true)
        : thread_count_(thread_count == 0 ? 1 : thread_count), verbose_logging_(verbose_logging) {}

    [[nodiscard]] ExecutionReport execute(const Dag& dag) const {
        ExecutionReport report;
        report.total_tasks = dag.size();

        const auto started = std::chrono::steady_clock::now();
        if (dag.size() == 0) {
            return report;
        }

        dag.validate();

        std::unordered_map<int, int> remaining = dag.indegree_map();
        std::queue<int> ready;
        for (const auto& [id, indegree] : remaining) {
            report.final_states[id] = TaskState::Pending;
            if (indegree == 0) {
                ready.push(id);
                report.final_states[id] = TaskState::Ready;
            }
        }

        std::mutex m;
        std::condition_variable cv;
        std::atomic<std::size_t> completed{0};
        bool stop = false;
        std::size_t sequence = 0;
        std::exception_ptr first_error;

        auto append_event = [&](int task_id, TaskState state, const std::string& message) {
            const auto now = std::chrono::steady_clock::now();
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - started).count();
            TaskEvent event{++sequence, task_id, state, elapsed, message};
            report.events.push_back(event);
            if (verbose_logging_) {
                std::cout << "[" << std::setw(4) << event.timestamp_ms << "ms] #" << event.sequence
                          << " task=" << event.task_id << " -> " << to_string(event.state);
                if (!event.message.empty()) {
                    std::cout << " (" << event.message << ")";
                }
                std::cout << '\n';
            }
        };

        auto worker = [&]() {
            while (true) {
                int task_id = -1;
                {
                    std::unique_lock<std::mutex> lock(m);
                    cv.wait(lock, [&]() {
                        return stop || !ready.empty();
                    });

                    if (stop && ready.empty()) {
                        return;
                    }
                    if (ready.empty()) {
                        continue;
                    }

                    task_id = ready.front();
                    ready.pop();
                    report.final_states[task_id] = TaskState::Running;
                    append_event(task_id, TaskState::Running, "Старт выполнения");
                }

                try {
                    dag.task(task_id).action();
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(m);
                    report.final_states[task_id] = TaskState::Failed;
                    ++report.failed_tasks;
                    append_event(task_id, TaskState::Failed, e.what());
                    if (report.error_message.empty()) {
                        report.error_message = e.what();
                    }
                    if (!first_error) {
                        first_error = std::current_exception();
                    }
                    stop = true;
                    cv.notify_all();
                    return;
                } catch (...) {
                    std::lock_guard<std::mutex> lock(m);
                    report.final_states[task_id] = TaskState::Failed;
                    ++report.failed_tasks;
                    append_event(task_id, TaskState::Failed, "Неизвестная ошибка");
                    if (report.error_message.empty()) {
                        report.error_message = "Неизвестная ошибка";
                    }
                    if (!first_error) {
                        first_error = std::current_exception();
                    }
                    stop = true;
                    cv.notify_all();
                    return;
                }

                {
                    std::lock_guard<std::mutex> lock(m);
                    report.final_states[task_id] = TaskState::Completed;
                    ++report.succeeded_tasks;
                    append_event(task_id, TaskState::Completed, "Завершено успешно");

                    for (int next : dag.dependents(task_id)) {
                        int& in = remaining[next];
                        --in;
                        if (in == 0) {
                            ready.push(next);
                            report.final_states[next] = TaskState::Ready;
                            append_event(next, TaskState::Ready, "Все зависимости выполнены");
                        }
                    }
                    const auto done = ++completed;
                    if (done == dag.size()) {
                        stop = true;
                    }
                }
                cv.notify_all();
            }
        };

        std::vector<std::thread> pool;
        pool.reserve(thread_count_);
        for (std::size_t i = 0; i < thread_count_; ++i) {
            pool.emplace_back(worker);
        }

        {
            std::lock_guard<std::mutex> lock(m);
            cv.notify_all();
        }

        for (auto& t : pool) {
            if (t.joinable()) {
                t.join();
            }
        }

        const auto finished = std::chrono::steady_clock::now();
        report.total_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(finished - started).count();

        if (!first_error && completed != dag.size()) {
            report.error_message = "Планировщик завершился до выполнения всех задач";
            throw std::runtime_error(report.error_message);
        }

        if (first_error) {
            std::rethrow_exception(first_error);
        }

        return report;
    }

private:
    std::size_t thread_count_;
    bool verbose_logging_;
};

inline Dag load_dag_from_file(const std::filesystem::path& path, const std::function<void(int)>& action_factory) {
    std::ifstream input(path);
    if (!input.is_open()) {
        throw std::runtime_error("Не удалось открыть файл графа: " + path.string());
    }

    enum class Section { None, Tasks, Dependencies };
    Section section = Section::None;

    Dag dag;
    std::string line;
    std::size_t line_no = 0;

    auto trim = [](std::string s) {
        const auto first = s.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) {
            return std::string{};
        }
        const auto last = s.find_last_not_of(" \t\r\n");
        return s.substr(first, last - first + 1);
    };

    while (std::getline(input, line)) {
        ++line_no;
        const std::string clean = trim(line);
        if (clean.empty() || clean[0] == '#') {
            continue;
        }

        if (clean == "[TASKS]") {
            section = Section::Tasks;
            continue;
        }
        if (clean == "[DEPENDENCIES]") {
            section = Section::Dependencies;
            continue;
        }

        std::istringstream iss(clean);
        if (section == Section::Tasks) {
            int id = 0;
            if (!(iss >> id)) {
                throw std::runtime_error("Ошибка парсинга id задачи в строке " + std::to_string(line_no));
            }
            dag.add_task({id, [action_factory, id] { action_factory(id); }});
            continue;
        }

        if (section == Section::Dependencies) {
            int from = 0;
            int to = 0;
            if (!(iss >> from >> to)) {
                throw std::runtime_error("Ошибка парсинга зависимости в строке " + std::to_string(line_no));
            }
            dag.add_dependency(from, to);
            continue;
        }

        throw std::runtime_error("Строка вне секции [TASKS]/[DEPENDENCIES], строка " + std::to_string(line_no));
    }

    dag.validate();
    return dag;
}

}  // namespace dag
