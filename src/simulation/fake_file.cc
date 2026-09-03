#include "src/simulation/fake_file.h"

#include "src/base/invariant.h"

#include <sys/stat.h>
#include <sys/uio.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <exception>
#include <functional>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace kwaque::simulation {

namespace {

constexpr invariant_id fake_dma_buffer_invariant{"KQ-FAKE-DMA-BUFFER"};
constexpr invariant_id fake_directory_transaction_invariant{
  "KQ-FAKE-DIRECTORY-TRANSACTION"};
constexpr invariant_id fake_storage_transaction_invariant{
  "KQ-FAKE-STORAGE-TRANSACTION"};
constexpr invariant_id fake_storage_drained_invariant{
  "KQ-FAKE-STORAGE-DRAINED"};

[[nodiscard]] runtime::operation_error file_error(errc code) noexcept {
    return runtime::operation_error{code, runtime::operation_kind::file};
}

[[nodiscard]] std::error_code native_error(errc code) noexcept {
    switch (code) {
    case errc::not_found:
        return std::make_error_code(std::errc::no_such_file_or_directory);
    case errc::already_exists:
        return std::make_error_code(std::errc::file_exists);
    case errc::permission_denied:
        return std::make_error_code(std::errc::permission_denied);
    case errc::directory_not_empty:
        return std::make_error_code(std::errc::directory_not_empty);
    case errc::is_a_directory:
        return std::make_error_code(std::errc::is_a_directory);
    case errc::not_a_directory:
        return std::make_error_code(std::errc::not_a_directory);
    case errc::aborted:
        return std::make_error_code(std::errc::operation_canceled);
    case errc::resource_exhausted:
    case errc::queue_full:
        return std::make_error_code(std::errc::no_space_on_device);
    case errc::out_of_range:
        return std::make_error_code(std::errc::file_too_large);
    case errc::invalid_argument:
        return std::make_error_code(std::errc::invalid_argument);
    default:
        return std::make_error_code(std::errc::io_error);
    }
}

template<typename T>
T native_value(runtime::result<T> result) {
    if (!result) {
        throw std::system_error(native_error(result.error().code()));
    }
    return std::move(*result);
}

[[nodiscard]] bool has_component_prefix(
  const std::vector<std::string>& value,
  const std::vector<std::string>& prefix) noexcept {
    return value.size() >= prefix.size()
           && std::equal(prefix.begin(), prefix.end(), value.begin());
}

[[nodiscard]] std::uint64_t page_count(std::uint64_t size) noexcept {
    return size / fake_file_page_bytes
           + static_cast<std::uint64_t>(size % fake_file_page_bytes != 0);
}

} // namespace

class fake_file_system::native_file_impl final : public seastar::file_impl {
public:
    native_file_impl(
      fake_file_system& owner,
      fake_object_id object,
      runtime::file_access access,
      std::uint64_t generation,
      seastar::lw_shared_ptr<open_handle_state> handle) noexcept
      : owner_(&owner)
      , object_(object)
      , access_(access)
      , generation_(generation)
      , handle_(std::move(handle)) {
        _memory_dma_alignment = owner.config_.memory_dma_alignment;
        _disk_read_dma_alignment = owner.config_.disk_read_dma_alignment;
        _disk_write_dma_alignment = owner.config_.disk_write_dma_alignment;
        _disk_overwrite_dma_alignment
          = owner.config_.disk_overwrite_dma_alignment;
        _read_max_length = owner.config_.native_max_length;
        _write_max_length = owner.config_.native_max_length;
    }

    native_file_impl(const native_file_impl&) = delete;
    native_file_impl& operator=(const native_file_impl&) = delete;
    native_file_impl(native_file_impl&&) = delete;
    native_file_impl& operator=(native_file_impl&&) = delete;

    ~native_file_impl() override {
        owner_->release_handle_reference(object_, handle_);
    }

    seastar::future<std::size_t> write_dma(
      std::uint64_t position,
      const void* buffer,
      std::size_t length,
      seastar::io_intent* intent) final {
        owner_->assert_current();
        handle_->assert_current();
        if (!valid_dma(
              pending_kind::write,
              position,
              buffer,
              length,
              _write_max_length)) {
            return seastar::make_exception_future<std::size_t>(
              std::system_error(native_error(errc::invalid_argument)));
        }
        if (access_ == runtime::file_access::read_only) {
            return seastar::make_exception_future<std::size_t>(
              std::system_error(native_error(errc::permission_denied)));
        }
        return owner_
          ->submit_file_operation(
            pending_kind::write,
            runtime::builtin_fault_point::file_write,
            object_,
            position,
            length,
            static_cast<const char*>(buffer),
            nullptr,
            intent,
            generation_)
          .then([](runtime::result<pending_value> outcome) {
              auto value = native_value(std::move(outcome));
              return static_cast<std::size_t>(
                std::get<byte_count>(value).value());
          });
    }

    seastar::future<std::size_t>
    write_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        owner_->assert_current();
        handle_->assert_current();
        return unsupported<std::size_t>();
    }

    seastar::future<std::size_t> read_dma(
      std::uint64_t position,
      void* buffer,
      std::size_t length,
      seastar::io_intent* intent) final {
        owner_->assert_current();
        handle_->assert_current();
        if (!valid_dma(
              pending_kind::read, position, buffer, length, _read_max_length)) {
            return seastar::make_exception_future<std::size_t>(
              std::system_error(native_error(errc::invalid_argument)));
        }
        if (access_ == runtime::file_access::write_only) {
            return seastar::make_exception_future<std::size_t>(
              std::system_error(native_error(errc::permission_denied)));
        }
        return owner_
          ->submit_file_operation(
            pending_kind::read,
            runtime::builtin_fault_point::file_read,
            object_,
            position,
            length,
            nullptr,
            static_cast<char*>(buffer),
            intent,
            generation_)
          .then([](runtime::result<pending_value> outcome) {
              auto value = native_value(std::move(outcome));
              return static_cast<std::size_t>(
                std::get<byte_count>(value).value());
          });
    }

    seastar::future<std::size_t>
    read_dma(std::uint64_t, std::vector<iovec>, seastar::io_intent*) final {
        owner_->assert_current();
        handle_->assert_current();
        return unsupported<std::size_t>();
    }

    seastar::future<seastar::temporary_buffer<std::uint8_t>> dma_read_bulk(
      std::uint64_t position,
      std::size_t length,
      seastar::io_intent* intent) final {
        owner_->assert_current();
        handle_->assert_current();
        if (handle_->lifecycle != handle_lifecycle::open) {
            return closed<seastar::temporary_buffer<std::uint8_t>>();
        }
        if (access_ == runtime::file_access::write_only) {
            return seastar::make_exception_future<
              seastar::temporary_buffer<std::uint8_t>>(
              std::system_error(native_error(errc::permission_denied)));
        }
        return owner_
          ->submit_file_operation(
            pending_kind::bulk_read,
            runtime::builtin_fault_point::file_read,
            object_,
            position,
            length,
            nullptr,
            nullptr,
            intent,
            generation_)
          .then([](runtime::result<pending_value> outcome) {
              auto value = native_value(std::move(outcome));
              return std::get<seastar::temporary_buffer<std::uint8_t>>(
                std::move(value));
          });
    }

    seastar::future<> flush() final {
        return void_operation(
          pending_kind::flush, runtime::builtin_fault_point::file_flush, 0);
    }

    seastar::future<struct stat> stat() final {
        return size().then([](std::uint64_t length) {
            struct stat status{};
            status.st_mode = S_IFREG | 0600;
            status.st_size = static_cast<off_t>(length);
            return status;
        });
    }

    seastar::future<> truncate(std::uint64_t length) final {
        owner_->assert_current();
        handle_->assert_current();
        if (access_ == runtime::file_access::read_only) {
            return seastar::make_exception_future<>(
              std::system_error(native_error(errc::permission_denied)));
        }
        return void_operation(
          pending_kind::truncate,
          runtime::builtin_fault_point::file_truncate,
          length);
    }

    seastar::future<> discard(std::uint64_t, std::uint64_t) final {
        owner_->assert_current();
        handle_->assert_current();
        return unsupported<>();
    }

    seastar::future<> allocate(std::uint64_t, std::uint64_t) final {
        owner_->assert_current();
        handle_->assert_current();
        return unsupported<>();
    }

    seastar::future<std::uint64_t> size() final {
        owner_->assert_current();
        handle_->assert_current();
        if (handle_->lifecycle != handle_lifecycle::open) {
            return closed<std::uint64_t>();
        }
        return owner_
          ->submit_file_operation(
            pending_kind::size,
            runtime::builtin_fault_point::file_size,
            object_,
            0,
            0,
            nullptr,
            nullptr,
            nullptr,
            generation_)
          .then([](runtime::result<pending_value> outcome) {
              return std::get<std::uint64_t>(native_value(std::move(outcome)));
          });
    }

    seastar::future<> close() final {
        owner_->assert_current();
        handle_->assert_current();
        if (handle_->lifecycle == handle_lifecycle::closed) {
            return seastar::make_ready_future<>();
        }
        if (generation_ != owner_->generation_) {
            owner_->release_handle_reference(object_, handle_);
            return seastar::make_ready_future<>();
        }
        if (handle_->lifecycle == handle_lifecycle::closing) {
            return closed<>();
        }
        handle_->lifecycle = handle_lifecycle::closing;
        return owner_
          ->submit_file_operation(
            pending_kind::close,
            runtime::builtin_fault_point::file_close,
            object_,
            0,
            0,
            nullptr,
            nullptr,
            nullptr,
            generation_,
            handle_)
          .then_wrapped(
            [owner = owner_, object = object_, handle = handle_](
              seastar::future<runtime::result<pending_value>> done) {
                try {
                    static_cast<void>(native_value(done.get()));
                    handle->lifecycle = handle_lifecycle::closed;
                } catch (...) {
                    // seastar::file::close() reports and swallows file_impl
                    // failures. Settle fake ownership here as successful close
                    // would, avoiding a misleading native close-error report.
                    owner->release_handle_reference(object, handle);
                }
            });
    }

    seastar::subscription<seastar::directory_entry> list_directory(
      std::function<seastar::future<>(seastar::directory_entry)> next) final {
        owner_->assert_current();
        handle_->assert_current();
        unsupported_listing_
          = std::make_unique<seastar::stream<seastar::directory_entry>>();
        auto subscription = unsupported_listing_->listen(std::move(next));
        unsupported_listing_->set_exception(
          std::system_error(
            std::make_error_code(std::errc::operation_not_supported)));
        return subscription;
    }

private:
    [[nodiscard]] bool valid_dma(
      pending_kind kind,
      std::uint64_t position,
      const void* buffer,
      std::size_t length,
      std::size_t maximum) const noexcept {
        const auto disk_alignment = kind != pending_kind::write
                                      ? _disk_read_dma_alignment
                                    : owner_->is_overwrite(
                                        object_, position, length)
                                      ? _disk_overwrite_dma_alignment
                                      : _disk_write_dma_alignment;
        return length != 0 && length <= maximum
               && handle_->lifecycle == handle_lifecycle::open
               && position % disk_alignment == 0 && length % disk_alignment == 0
               && std::bit_cast<std::uintptr_t>(buffer) % _memory_dma_alignment
                    == 0;
    }

    template<typename T = void>
    static seastar::future<T> unsupported() {
        return seastar::make_exception_future<T>(std::system_error(
          std::make_error_code(std::errc::operation_not_supported)));
    }

    template<typename T = void>
    static seastar::future<T> closed() {
        return seastar::make_exception_future<T>(std::system_error(
          std::make_error_code(std::errc::bad_file_descriptor)));
    }

    seastar::future<> void_operation(
      pending_kind kind,
      runtime::builtin_fault_point point,
      std::uint64_t value) {
        owner_->assert_current();
        handle_->assert_current();
        if (handle_->lifecycle != handle_lifecycle::open) {
            return closed<>();
        }
        return owner_
          ->submit_file_operation(
            kind,
            point,
            object_,
            value,
            0,
            nullptr,
            nullptr,
            nullptr,
            generation_)
          .then([](runtime::result<pending_value> outcome) {
              static_cast<void>(native_value(std::move(outcome)));
          });
    }

    fake_file_system* owner_;
    fake_object_id object_;
    runtime::file_access access_;
    std::uint64_t generation_;
    seastar::lw_shared_ptr<open_handle_state> handle_;
    std::unique_ptr<seastar::stream<seastar::directory_entry>>
      unsupported_listing_;
};

fake_file_system::pending_table::pending_table(std::size_t capacity) {
    auto remaining = capacity;
    while (remaining != 0) {
        const auto count = std::min(remaining, entries_per_chunk);
        chunks_.emplace_back(count);
        remaining -= count;
    }
    indices_.reserve(capacity);
    free_slots_.reserve(capacity);
    for (std::size_t index = capacity; index != 0; --index) {
        free_slots_.push_back(index - 1U);
    }
}

fake_file_system::pending_table::slot&
fake_file_system::pending_table::slot_at(std::size_t index) noexcept {
    return chunks_[index / entries_per_chunk][index % entries_per_chunk];
}

std::pair<fake_file_system::pending_operation*, bool>
fake_file_system::pending_table::try_emplace(
  std::uint64_t key, prepared_operation operation) {
    if (indices_.contains(key)) {
        return {nullptr, false};
    }
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      !free_slots_.empty(),
      "fake pending table exceeded its fixed capacity");
    const auto index = free_slots_.back();
    free_slots_.pop_back();
    auto& selected = slot_at(index);
    selected.emplace(std::move(operation));
    try {
        const auto [position, inserted] = indices_.try_emplace(key, index);
        static_cast<void>(position);
        if (!inserted) {
            selected.reset();
            free_slots_.push_back(index);
            return {nullptr, false};
        }
    } catch (...) {
        selected.reset();
        free_slots_.push_back(index);
        throw;
    }
    return {&*selected, true};
}

fake_file_system::pending_operation&
fake_file_system::pending_table::at(std::uint64_t key) noexcept {
    auto* found = find(key);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      found != nullptr,
      "fake pending table lost a live operation");
    return *found;
}

fake_file_system::pending_operation*
fake_file_system::pending_table::find(std::uint64_t key) noexcept {
    const auto found = indices_.find(key);
    if (found == indices_.end()) {
        return nullptr;
    }
    auto& operation = *slot_at(found->second);
    operation.assert_current();
    return &operation;
}

bool fake_file_system::pending_table::erase(std::uint64_t key) noexcept {
    const auto found = indices_.find(key);
    if (found == indices_.end()) {
        return false;
    }
    const auto index = found->second;
    indices_.erase(found);
    slot_at(index).reset();
    free_slots_.push_back(index);
    return true;
}

void fake_file_system::pending_table::copy_keys(
  std::vector<std::uint64_t>& output) const noexcept {
    output.clear();
    for (const auto& chunk : chunks_) {
        for (const auto& slot : chunk) {
            if (slot) {
                KWAQUE_INVARIANT(
                  fake_storage_transaction_invariant,
                  output.size() != output.capacity(),
                  "fake pending-key worklist exceeded reserved capacity");
                output.push_back(slot->id.value());
            }
        }
    }
}

fake_file_system::object_worklist::object_worklist(std::size_t capacity)
  : capacity_(capacity) {
    auto remaining = capacity;
    while (remaining != 0) {
        const auto count = std::min(remaining, entries_per_chunk);
        chunks_.emplace_back(count);
        remaining -= count;
    }
}

void fake_file_system::object_worklist::push_back(std::uint64_t id) noexcept {
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      size_ < capacity_,
      "fake object collection worklist exceeded its fixed capacity");
    chunks_[size_ / entries_per_chunk][size_ % entries_per_chunk] = id;
    ++size_;
}

std::uint64_t fake_file_system::object_worklist::operator[](
  std::size_t index) const noexcept {
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      index < size_,
      "fake object collection worklist index out of range");
    return chunks_[index / entries_per_chunk][index % entries_per_chunk];
}

runtime::result<canonical_fake_path> fake_file_system::canonicalize(
  std::string_view input,
  const std::vector<std::string>& base,
  std::size_t minimum_depth,
  bool require_absolute) {
    if (input.size() > fake_path_bytes_max) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    if (
      input.empty() || input.find('\0') != std::string_view::npos
      || (require_absolute && input.front() != '/')) {
        return runtime::failure(file_error(errc::invalid_argument));
    }

    std::vector<std::string> components = input.front() == '/'
                                            ? std::vector<std::string>{}
                                            : base;
    const auto floor = input.front() == '/' ? std::size_t{0} : minimum_depth;
    std::size_t begin = 0;
    while (begin < input.size()) {
        while (begin < input.size() && input[begin] == '/') {
            ++begin;
        }
        if (begin == input.size()) {
            break;
        }
        const auto separator = input.find('/', begin);
        const auto end = separator == std::string_view::npos ? input.size()
                                                             : separator;
        const auto component = input.substr(begin, end - begin);
        begin = end;
        if (component == ".") {
            continue;
        }
        if (component == "..") {
            if (components.size() == floor) {
                return runtime::failure(file_error(errc::invalid_argument));
            }
            components.pop_back();
            continue;
        }
        if (component.size() > fake_path_component_bytes_max) {
            return runtime::failure(file_error(errc::out_of_range));
        }
        components.emplace_back(component);
    }

    std::size_t bytes = 1;
    for (const auto& component : components) {
        bytes += component.size() + static_cast<std::size_t>(bytes != 1);
    }
    if (bytes > fake_path_bytes_max) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    std::string canonical;
    canonical.reserve(bytes);
    canonical.push_back('/');
    for (std::size_t index = 0; index < components.size(); ++index) {
        if (index != 0) {
            canonical.push_back('/');
        }
        canonical.append(components[index]);
    }
    return canonical_fake_path{std::move(canonical), std::move(components)};
}

std::uint64_t
fake_file_system::retained_path_size(const canonical_fake_path& path) noexcept {
    std::uint64_t retained = path.bytes().size();
    for (const auto& component : path.components()) {
        retained += component.size();
    }
    return retained;
}

bool fake_file_system::unsigned_name_less::operator()(
  std::string_view left, std::string_view right) const noexcept {
    return std::lexicographical_compare(
      left.begin(),
      left.end(),
      right.begin(),
      right.end(),
      [](char lhs, char rhs) {
          return static_cast<unsigned char>(lhs)
                 < static_cast<unsigned char>(rhs);
      });
}

runtime::result<canonical_fake_path>
fake_file_system::canonicalize_root(std::string_view root) {
    return canonicalize(root, {}, 0, true);
}

runtime::result<std::unique_ptr<fake_file_system>>
fake_file_system::make(fake_file_system_config config) {
    if (
      config.logical_capacity.value() == 0
      || config.logical_capacity > maximum_fake_disk_capacity
      || config.maximum_objects == 0
      || config.maximum_objects > maximum_fake_file_objects
      || config.maximum_operation_bytes.value() == 0
      || config.maximum_operation_bytes > runtime::maximum_file_io_bytes
      || config.maximum_retained_path_bytes.value() == 0
      || config.maximum_retained_path_bytes > maximum_fake_retained_path_bytes
      || config.maximum_open_handles == 0
      || config.maximum_open_handles > maximum_fake_open_handles
      || config.maximum_pending_operations == 0
      || config.maximum_pending_operations > maximum_fake_pending_operations
      || config.maximum_pending_bytes.value() == 0
      || config.maximum_pending_bytes > maximum_fake_pending_bytes
      || config.base_latency.nanoseconds() == 0
      || config.read_latency_mean < config.read_latency_min
      || config.write_latency_mean < config.write_latency_min
      || config.maximum_pending_reads == 0
      || config.maximum_pending_reads > maximum_fake_pending_operations
      || config.maximum_pending_writes == 0
      || config.maximum_pending_writes > maximum_fake_pending_operations
      || !std::has_single_bit(config.memory_dma_alignment)
      || !std::has_single_bit(config.disk_read_dma_alignment)
      || !std::has_single_bit(config.disk_write_dma_alignment)
      || !std::has_single_bit(config.disk_overwrite_dma_alignment)
      || config.memory_dma_alignment > maximum_contiguous_allocation_bytes
      || config.disk_read_dma_alignment > maximum_contiguous_allocation_bytes
      || config.disk_write_dma_alignment > maximum_contiguous_allocation_bytes
      || config.disk_overwrite_dma_alignment
           > maximum_contiguous_allocation_bytes
      || config.native_max_length == 0
      || config.native_max_length > maximum_contiguous_allocation_bytes
      || config.native_max_length % config.disk_read_dma_alignment != 0
      || config.native_max_length % config.disk_write_dma_alignment != 0
      || config.native_max_length % config.disk_overwrite_dma_alignment != 0) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    auto root = canonicalize_root(config.virtual_root);
    if (!root) {
        return runtime::failure(root.error());
    }
    if (
      retained_path_size(*root) > config.maximum_retained_path_bytes.value()) {
        return runtime::failure(file_error(errc::resource_exhausted));
    }
    std::string{}.swap(config.virtual_root);
    return std::unique_ptr<fake_file_system>{new fake_file_system{
      std::move(config), std::move(*root), nullptr, nullptr}};
}

runtime::result<std::unique_ptr<fake_file_system>> fake_file_system::make(
  fake_file_system_config config,
  scheduler& event_scheduler,
  fault_schedule& faults) {
    auto state = make(std::move(config));
    if (!state) {
        return runtime::failure(state.error());
    }
    event_scheduler.assert_current();
    faults.assert_current();
    const auto first_deadline = event_scheduler.now().checked_add(
      (*state)->config_.base_latency);
    if (
      event_scheduler.limits().pending_events()
        < static_cast<std::uint64_t>(
            (*state)->config_.maximum_pending_operations)
            * 3U
      || !first_deadline
      || *first_deadline > event_scheduler.limits().maximum_deadline()) {
        return runtime::failure(file_error(errc::invalid_argument));
    }
    (*state)->scheduler_ = &event_scheduler;
    (*state)->faults_ = &faults;
    return state;
}

fake_file_system::fake_file_system(
  fake_file_system_config config,
  canonical_fake_path root,
  scheduler* event_scheduler,
  fault_schedule* faults)
  : config_(std::move(config))
  , root_(std::move(root))
  , scheduler_(event_scheduler)
  , faults_(faults)
  , pending_(config_.maximum_pending_operations)
  , collection_worklist_(std::size_t{3} * config_.maximum_objects) {
    pending_ids_.reserve(config_.maximum_pending_operations);
    open_objects_.reserve(config_.maximum_open_handles);
    retained_path_bytes_ = retained_path_size(root_);
    const fake_object_id root_id{1};
    objects_.try_emplace(
      root_id.value(),
      std::make_unique<inode>(
        root_id,
        fake_file_kind::directory,
        std::variant<regular_file_state, directory_state>{
          std::in_place_type<directory_state>},
        1,
        1));
}

fake_file_system::~fake_file_system() {
    assert_current();
    if (scheduler_ != nullptr && scheduler_->trace_failed()) {
        static_cast<void>(scheduler_->discard_failed());
        if (
          const auto* failure = scheduler_->trace_failure();
          failure != nullptr) {
            discard_remaining(*failure);
        }
    }
    KWAQUE_INVARIANT(
      fake_storage_drained_invariant,
      pending_operations_ == 0 && pending_bytes_.value() == 0
        && pending_path_bytes_ == 0 && pending_opens_ == 0
        && pending_reads_ == 0 && pending_writes_ == 0
        && parked_operations_ == 0,
      "fake filesystem destroyed with pending work");
}

seastar::future<runtime::result<runtime::file>> fake_file_system::open(
  runtime::file_path path, runtime::file_open_options options) {
    assert_current();
    if (auto valid = options.validate(); !valid) {
        return seastar::make_ready_future<runtime::result<runtime::file>>(
          runtime::failure(valid.error()));
    }
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<runtime::result<runtime::file>>(
          runtime::failure(canonical.error()));
    }
    auto existing = lookup(*canonical);
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::open,
      runtime::builtin_fault_point::file_open};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    metadata.open_options = options;
    operation.open_slot = true;
    if (existing) {
        operation.object = *existing;
    }
    const auto object_key = existing ? runtime::fault_object_key::from_u64(
                                         existing->value())
                                     : runtime::fault_object_key::none();
    return submit(
             std::move(operation),
             object_key,
             byte_count{},
             trace_event_descriptor{
               .kind = trace_event_kind::filesystem,
               .stable_id = next_operation_id_,
             })
      .then(
        [](runtime::result<pending_value> outcome)
          -> runtime::result<runtime::file> {
            if (!outcome) {
                return runtime::failure(outcome.error());
            }
            return std::get<runtime::file>(std::move(*outcome));
        });
}

seastar::future<runtime::result<bool>>
fake_file_system::exists(runtime::file_path path) {
    assert_current();
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<runtime::result<bool>>(
          runtime::failure(canonical.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::exists,
      runtime::builtin_fault_point::file_exists};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return submit(
             std::move(operation),
             existing ? runtime::fault_object_key::from_u64(existing->value())
                      : runtime::fault_object_key::none(),
             byte_count{},
             trace_event_descriptor{
               .kind = trace_event_kind::filesystem,
               .stable_id = next_operation_id_,
             })
      .then(
        [](runtime::result<pending_value> outcome) -> runtime::result<bool> {
            if (!outcome) {
                return runtime::failure(outcome.error());
            }
            return std::get<bool>(*outcome);
        });
}

seastar::future<runtime::result<runtime::file_status>>
fake_file_system::stat(runtime::file_path path) {
    assert_current();
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<
          runtime::result<runtime::file_status>>(
          runtime::failure(canonical.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::stat,
      runtime::builtin_fault_point::file_stat};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return submit(
             std::move(operation),
             existing ? runtime::fault_object_key::from_u64(existing->value())
                      : runtime::fault_object_key::none(),
             byte_count{},
             trace_event_descriptor{
               .kind = trace_event_kind::filesystem,
               .stable_id = next_operation_id_,
             })
      .then(
        [](runtime::result<pending_value> outcome)
          -> runtime::result<runtime::file_status> {
            if (!outcome) {
                return runtime::failure(outcome.error());
            }
            return std::get<runtime::file_status>(*outcome);
        });
}

seastar::future<runtime::result<runtime::directory_listing>>
fake_file_system::list(
  runtime::file_path path, runtime::directory_listing_limits limits) {
    assert_current();
    if (auto valid = limits.validate(); !valid) {
        return seastar::make_ready_future<
          runtime::result<runtime::directory_listing>>(
          runtime::failure(valid.error()));
    }
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<
          runtime::result<runtime::directory_listing>>(
          runtime::failure(canonical.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::list,
      runtime::builtin_fault_point::file_list};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    metadata.listing_limits = limits;
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return submit(
             std::move(operation),
             existing ? runtime::fault_object_key::from_u64(existing->value())
                      : runtime::fault_object_key::none(),
             byte_count{},
             trace_event_descriptor{
               .kind = trace_event_kind::filesystem,
               .stable_id = next_operation_id_,
             })
      .then(
        [](runtime::result<pending_value> outcome)
          -> runtime::result<runtime::directory_listing> {
            if (!outcome) {
                return runtime::failure(outcome.error());
            }
            return std::get<runtime::directory_listing>(std::move(*outcome));
        });
}

namespace {

template<typename Result>
seastar::future<runtime::result<void>>
discard_pending_value(seastar::future<runtime::result<Result>> future) {
    return std::move(future).then(
      [](runtime::result<Result> outcome) -> runtime::result<void> {
          if (!outcome) {
              return runtime::failure(outcome.error());
          }
          return {};
      });
}

} // namespace

seastar::future<runtime::result<void>> fake_file_system::crash() {
    assert_current();
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::crash_control,
      runtime::builtin_fault_point::file};
    return discard_pending_value(submit(
      std::move(operation),
      runtime::fault_object_key::none(),
      byte_count{},
      trace_event_descriptor{
        .kind = trace_event_kind::filesystem,
        .stable_id = next_operation_id_,
      }));
}

seastar::future<runtime::result<void>> fake_file_system::stop() {
    assert_current();
    if (state_ == fake_file_system_state::stopping) {
        return stop_done_->get_shared_future();
    }
    if (state_ == fake_file_system_state::stopped) {
        return stop_done_ && stop_done_->available()
                 ? stop_done_->get_shared_future()
                 : seastar::make_ready_future<runtime::result<void>>(
                     runtime::result<void>{});
    }

    try {
        stop_done_.emplace();
    } catch (...) {
        return seastar::current_exception_as_future<runtime::result<void>>();
    }
    state_ = fake_file_system_state::stopping;
    operation_changed_.broadcast();
    if (scheduler_ == nullptr) {
        invalidate_handles();
        finish_stop();
        return stop_done_->get_shared_future();
    }
    pending_.copy_keys(pending_ids_);
    std::ranges::sort(pending_ids_);
    for (const auto value : pending_ids_) {
        auto* operation = pending_.find(value);
        if (operation == nullptr) {
            continue;
        }
        if (auto scheduled = schedule_terminal(*operation); !scheduled) {
            if (!stop_failure_) {
                stop_failure_.emplace(scheduled.error());
            }
            KWAQUE_INVARIANT(
              fake_storage_transaction_invariant,
              scheduler_->trace_failed(),
              "reserved fake stop terminal event failed without divergence");
            break;
        }
    }

    if (scheduler_->trace_failed()) {
        if (!stop_failure_) {
            stop_failure_.emplace(*scheduler_->trace_failure());
        }
        static_cast<void>(scheduler_->discard_failed());
        discard_remaining(*scheduler_->trace_failure());
        invalidate_handles();
    } else {
        invalidate_handles();
    }
    if (
      state_ == fake_file_system_state::stopping && pending_operations_ == 0) {
        finish_stop();
    }
    return stop_done_->get_shared_future();
}

seastar::future<runtime::result<void>>
fake_file_system::create_directories(runtime::file_path path) {
    assert_current();
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(canonical.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::create_directories,
      runtime::builtin_fault_point::directory_create};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return discard_pending_value(submit(
      std::move(operation),
      existing ? runtime::fault_object_key::from_u64(existing->value())
               : runtime::fault_object_key::none(),
      byte_count{},
      trace_event_descriptor{
        .kind = trace_event_kind::filesystem,
        .stable_id = next_operation_id_,
      }));
}

seastar::future<runtime::result<void>>
fake_file_system::remove_file(runtime::file_path path) {
    assert_current();
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(canonical.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::remove_file,
      runtime::builtin_fault_point::file_remove};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return discard_pending_value(submit(
      std::move(operation),
      existing ? runtime::fault_object_key::from_u64(existing->value())
               : runtime::fault_object_key::none(),
      byte_count{},
      trace_event_descriptor{
        .kind = trace_event_kind::filesystem,
        .stable_id = next_operation_id_,
      }));
}

seastar::future<runtime::result<void>>
fake_file_system::remove_directory(runtime::file_path path) {
    assert_current();
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(canonical.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::remove_directory,
      runtime::builtin_fault_point::directory_remove};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return discard_pending_value(submit(
      std::move(operation),
      existing ? runtime::fault_object_key::from_u64(existing->value())
               : runtime::fault_object_key::none(),
      byte_count{},
      trace_event_descriptor{
        .kind = trace_event_kind::filesystem,
        .stable_id = next_operation_id_,
      }));
}

seastar::future<runtime::result<void>> fake_file_system::rename(
  runtime::file_path source, runtime::file_path destination) {
    assert_current();
    auto from = resolve(source.value());
    auto to = resolve(destination.value());
    if (!from || !to) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(!from ? from.error() : to.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::rename,
      runtime::builtin_fault_point::file_rename};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*from);
    metadata.destination_path = std::move(*to);
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return discard_pending_value(submit(
      std::move(operation),
      existing ? runtime::fault_object_key::from_u64(existing->value())
               : runtime::fault_object_key::none(),
      byte_count{},
      trace_event_descriptor{
        .kind = trace_event_kind::filesystem,
        .stable_id = next_operation_id_,
      }));
}

seastar::future<runtime::result<void>>
fake_file_system::sync_directory(runtime::file_path path) {
    assert_current();
    auto canonical = resolve(path.value());
    if (!canonical) {
        return seastar::make_ready_future<runtime::result<void>>(
          runtime::failure(canonical.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_},
      pending_kind::sync_directory,
      runtime::builtin_fault_point::directory_sync};
    auto& metadata = operation.payload.emplace<metadata_operation>();
    metadata.path = std::move(*canonical);
    const auto existing = lookup(*metadata.path);
    if (existing) {
        operation.object = *existing;
    }
    return discard_pending_value(submit(
      std::move(operation),
      existing ? runtime::fault_object_key::from_u64(existing->value())
               : runtime::fault_object_key::none(),
      byte_count{},
      trace_event_descriptor{
        .kind = trace_event_kind::filesystem,
        .stable_id = next_operation_id_,
      }));
}

runtime::result<canonical_fake_path>
fake_file_system::resolve(std::string_view input) const {
    assert_current();
    auto resolved = canonicalize(
      input, root_.components(), root_.components().size(), false);
    if (!resolved) {
        return runtime::failure(resolved.error());
    }
    if (!has_component_prefix(resolved->components(), root_.components())) {
        return runtime::failure(file_error(errc::invalid_argument));
    }
    return resolved;
}

fake_file_system::inode*
fake_file_system::find_inode(fake_object_id id) noexcept {
    const auto found = objects_.find(id.value());
    if (found == objects_.end()) {
        return nullptr;
    }
    found->second->assert_current();
    return found->second.get();
}

const fake_file_system::inode*
fake_file_system::find_inode(fake_object_id id) const noexcept {
    const auto found = objects_.find(id.value());
    if (found == objects_.end()) {
        return nullptr;
    }
    found->second->assert_current();
    return found->second.get();
}

bool fake_file_system::is_overwrite(
  fake_object_id id,
  std::uint64_t position,
  std::uint64_t length) const noexcept {
    assert_current();
    const auto* selected = find_inode(id);
    if (selected == nullptr || selected->kind != fake_file_kind::regular) {
        return false;
    }
    const auto size
      = std::get<regular_file_state>(selected->state).visible_size;
    return position <= size && length <= size - position;
}

runtime::result<fake_object_id>
fake_file_system::lookup(const canonical_fake_path& path) const noexcept {
    assert_current();
    if (!has_component_prefix(path.components(), root_.components())) {
        return runtime::failure(file_error(errc::invalid_argument));
    }
    fake_object_id current{1};
    for (std::size_t index = root_.components().size();
         index < path.components().size();
         ++index) {
        const auto* owner = find_inode(current);
        if (owner == nullptr) {
            return runtime::failure(file_error(errc::not_found));
        }
        if (owner->kind != fake_file_kind::directory) {
            return runtime::failure(file_error(errc::not_a_directory));
        }
        const auto child = visible_child(
          std::get<directory_state>(owner->state), path.components()[index]);
        if (!child) {
            return runtime::failure(file_error(errc::not_found));
        }
        current = *child;
    }
    return current;
}

runtime::result<fake_object_id> fake_file_system::lookup_parent(
  const canonical_fake_path& path) const noexcept {
    assert_current();
    if (path.components().size() <= root_.components().size()) {
        return runtime::failure(file_error(errc::invalid_argument));
    }
    fake_object_id current{1};
    for (std::size_t index = root_.components().size();
         index + 1U < path.components().size();
         ++index) {
        const auto* owner = find_inode(current);
        if (owner == nullptr || owner->kind != fake_file_kind::directory) {
            return runtime::failure(file_error(errc::not_a_directory));
        }
        const auto child = visible_child(
          std::get<directory_state>(owner->state), path.components()[index]);
        if (!child) {
            return runtime::failure(file_error(errc::not_found));
        }
        current = *child;
    }
    const auto* parent = find_inode(current);
    if (parent == nullptr || parent->kind != fake_file_kind::directory) {
        return runtime::failure(file_error(errc::not_a_directory));
    }
    return current;
}

std::optional<fake_object_id> fake_file_system::visible_child(
  const directory_state& directory, std::string_view name) noexcept {
    if (
      const auto changed = directory.unsynced.find(name);
      changed != directory.unsynced.end()) {
        return changed->second;
    }
    const auto durable = directory.durable.find(name);
    return durable == directory.durable.end()
             ? std::optional<fake_object_id>{}
             : std::optional<fake_object_id>{durable->second};
}

bool fake_file_system::directory_visible_empty(
  const directory_state& directory) const noexcept {
    for (const auto& [name, child] : directory.durable) {
        static_cast<void>(child);
        if (visible_child(directory, name)) {
            return false;
        }
    }
    for (const auto& [name, child] : directory.unsynced) {
        if (child && !directory.durable.contains(name)) {
            return false;
        }
    }
    return true;
}

std::int64_t fake_file_system::directory_change_path_delta(
  const directory_state& directory, std::string_view name) noexcept {
    return directory.unsynced.contains(name)
             ? 0
             : static_cast<std::int64_t>(name.size());
}

fake_file_system::prepared_directory_change::prepared_directory_change(
  fake_file_system& owner,
  fake_object_id directory_id,
  directory_state& directory,
  std::string name,
  std::optional<fake_object_id> replacement)
  : owner_(&owner)
  , directory_id_(directory_id)
  , directory_(&directory)
  , name_(std::move(name))
  , replacement_(replacement)
  , path_byte_delta_(directory_change_path_delta(directory, name_)) {
    previous_visible_ = visible_child(*directory_, name_);
    const auto unsynced = directory_->unsynced.find(name_);
    had_unsynced_ = unsynced != directory_->unsynced.end();

    try {
        if (!had_unsynced_) {
            const auto [position, inserted] = directory_->unsynced.try_emplace(
              name_, replacement_);
            static_cast<void>(position);
            KWAQUE_INVARIANT(
              fake_directory_transaction_invariant,
              inserted,
              "directory delta placeholder was not inserted");
            inserted_unsynced_ = true;
        }
    } catch (...) {
        rollback();
        throw;
    }
}

fake_file_system::prepared_directory_change::~prepared_directory_change() {
    rollback();
}

fake_file_system::prepared_directory_change::prepared_directory_change(
  prepared_directory_change&& other) noexcept
  : owner_(std::exchange(other.owner_, nullptr))
  , directory_id_(other.directory_id_)
  , directory_(std::exchange(other.directory_, nullptr))
  , name_(std::move(other.name_))
  , replacement_(other.replacement_)
  , previous_visible_(other.previous_visible_)
  , had_unsynced_(other.had_unsynced_)
  , inserted_unsynced_(std::exchange(other.inserted_unsynced_, false))
  , committed_(std::exchange(other.committed_, true))
  , path_byte_delta_(other.path_byte_delta_) {}

fake_file_system::prepared_directory_change&
fake_file_system::prepared_directory_change::operator=(
  prepared_directory_change&& other) noexcept {
    if (this != &other) {
        rollback();
        owner_ = std::exchange(other.owner_, nullptr);
        directory_id_ = other.directory_id_;
        directory_ = std::exchange(other.directory_, nullptr);
        name_ = std::move(other.name_);
        replacement_ = other.replacement_;
        previous_visible_ = other.previous_visible_;
        had_unsynced_ = other.had_unsynced_;
        inserted_unsynced_ = std::exchange(other.inserted_unsynced_, false);
        committed_ = std::exchange(other.committed_, true);
        path_byte_delta_ = other.path_byte_delta_;
    }
    return *this;
}

void fake_file_system::prepared_directory_change::commit() noexcept {
    KWAQUE_INVARIANT(
      fake_directory_transaction_invariant,
      owner_ != nullptr && directory_ != nullptr && !committed_,
      "directory change committed twice");
    if (
      previous_visible_
      && (!replacement_ || *previous_visible_ != *replacement_)) {
        --owner_->find_inode(*previous_visible_)->visible_links;
    }
    if (
      replacement_
      && (!previous_visible_ || *previous_visible_ != *replacement_)) {
        ++owner_->find_inode(*replacement_)->visible_links;
    }
    directory_->unsynced.find(name_)->second = replacement_;
    owner_->apply_path_delta(path_byte_delta_);
    owner_->mark_dirty(*owner_->find_inode(directory_id_));
    committed_ = true;
}

void fake_file_system::prepared_directory_change::rollback() noexcept {
    if (committed_ || directory_ == nullptr) {
        return;
    }
    if (inserted_unsynced_) {
        directory_->unsynced.erase(name_);
        inserted_unsynced_ = false;
    }
}

runtime::result<void>
fake_file_system::validate_path_delta(std::int64_t delta) const noexcept {
    if (delta <= 0) {
        return {};
    }
    const auto added = static_cast<std::uint64_t>(delta);
    const auto maximum = config_.maximum_retained_path_bytes.value();
    if (
      retained_path_bytes_ > maximum - pending_path_bytes_
      || added > maximum - pending_path_bytes_ - retained_path_bytes_) {
        return runtime::failure(file_error(errc::resource_exhausted));
    }
    return {};
}

void fake_file_system::apply_path_delta(std::int64_t delta) noexcept {
    if (delta >= 0) {
        retained_path_bytes_ += static_cast<std::uint64_t>(delta);
    } else {
        const auto removed = static_cast<std::uint64_t>(-delta);
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          removed <= retained_path_bytes_,
          "fake namespace path accounting underflow");
        retained_path_bytes_ -= removed;
    }
}

runtime::result<void> fake_file_system::apply_directory_change(
  fake_object_id directory_id,
  directory_state& directory,
  std::string name,
  std::optional<fake_object_id> id) {
    const auto delta = directory_change_path_delta(directory, name);
    if (auto valid = validate_path_delta(delta); !valid) {
        return runtime::failure(valid.error());
    }
    prepared_directory_change change{
      *this, directory_id, directory, std::move(name), id};
    change.commit();
    return {};
}

runtime::result<fake_object_id>
fake_file_system::create(const canonical_fake_path& path, fake_file_kind kind) {
    assert_current();
    if (object_ids_exhausted_ || objects_.size() >= config_.maximum_objects) {
        return runtime::failure(file_error(errc::resource_exhausted));
    }
    auto parent_id = lookup_parent(path);
    if (!parent_id) {
        return runtime::failure(parent_id.error());
    }
    auto* parent = find_inode(*parent_id);
    auto& children = std::get<directory_state>(parent->state);
    const auto& name = path.components().back();
    if (visible_child(children, name)) {
        return runtime::failure(file_error(errc::already_exists));
    }
    const fake_object_id id{next_object_id_};
    if (
      auto valid = validate_path_delta(
        directory_change_path_delta(children, name));
      !valid) {
        return runtime::failure(valid.error());
    }
    std::variant<regular_file_state, directory_state> state;
    if (kind == fake_file_kind::regular) {
        state.emplace<regular_file_state>();
    } else {
        state.emplace<directory_state>();
    }
    objects_.reserve(objects_.size() + 1U);
    const auto [position, inserted] = objects_.try_emplace(
      id.value(), std::make_unique<inode>(id, kind, std::move(state)));
    static_cast<void>(position);
    KWAQUE_INVARIANT(
      fake_directory_transaction_invariant,
      inserted,
      "new fake object ID was already present");
    try {
        if (
          auto linked = apply_directory_change(*parent_id, children, name, id);
          !linked) {
            objects_.erase(id.value());
            return runtime::failure(linked.error());
        }
    } catch (...) {
        objects_.erase(id.value());
        throw;
    }
    if (next_object_id_ == std::numeric_limits<std::uint64_t>::max()) {
        object_ids_exhausted_ = true;
    } else {
        ++next_object_id_;
    }
    return id;
}

runtime::result<void>
fake_file_system::remove(const canonical_fake_path& path, fake_file_kind kind) {
    assert_current();
    auto parent_id = lookup_parent(path);
    if (!parent_id) {
        return runtime::failure(parent_id.error());
    }
    auto* parent = find_inode(*parent_id);
    auto& children = std::get<directory_state>(parent->state);
    const auto& name = path.components().back();
    const auto found = visible_child(children, name);
    if (!found) {
        return runtime::failure(file_error(errc::not_found));
    }
    const auto target_id = *found;
    auto* target = find_inode(target_id);
    if (target->kind != kind) {
        return runtime::failure(file_error(
          kind == fake_file_kind::directory ? errc::not_a_directory
                                            : errc::is_a_directory));
    }
    if (
      kind == fake_file_kind::directory
      && !directory_visible_empty(std::get<directory_state>(target->state))) {
        return runtime::failure(file_error(errc::directory_not_empty));
    }
    if (
      auto unlinked = apply_directory_change(
        *parent_id, children, name, std::nullopt);
      !unlinked) {
        return runtime::failure(unlinked.error());
    }
    collect_unreachable(target_id);
    return {};
}

runtime::result<void> fake_file_system::rename(
  const canonical_fake_path& from, const canonical_fake_path& to) {
    assert_current();
    if (from == to) {
        return {};
    }
    auto from_parent_id = lookup_parent(from);
    auto to_parent_id = lookup_parent(to);
    if (!from_parent_id) {
        return runtime::failure(from_parent_id.error());
    }
    if (!to_parent_id) {
        return runtime::failure(to_parent_id.error());
    }
    auto& from_directory = std::get<directory_state>(
      find_inode(*from_parent_id)->state);
    auto& to_directory = std::get<directory_state>(
      find_inode(*to_parent_id)->state);
    const auto& from_name = from.components().back();
    const auto& to_name = to.components().back();
    const auto source_position = visible_child(from_directory, from_name);
    if (!source_position) {
        return runtime::failure(file_error(errc::not_found));
    }
    const auto source_id = *source_position;
    auto* source = find_inode(source_id);
    if (
      source->kind == fake_file_kind::directory
      && to.components().size() > from.components().size()
      && std::equal(
        from.components().begin(),
        from.components().end(),
        to.components().begin())) {
        return runtime::failure(file_error(errc::invalid_argument));
    }

    const auto target_position = visible_child(to_directory, to_name);
    std::optional<fake_object_id> replaced = target_position;
    if (target_position) {
        auto* target = find_inode(*target_position);
        if (target->kind != source->kind) {
            return runtime::failure(file_error(
              source->kind == fake_file_kind::directory
                ? errc::not_a_directory
                : errc::is_a_directory));
        }
        if (
          target->kind == fake_file_kind::directory
          && !directory_visible_empty(
            std::get<directory_state>(target->state))) {
            return runtime::failure(file_error(errc::directory_not_empty));
        }
    }

    const auto path_delta
      = directory_change_path_delta(from_directory, from_name)
        + directory_change_path_delta(to_directory, to_name);
    if (auto valid = validate_path_delta(path_delta); !valid) {
        return runtime::failure(valid.error());
    }
    prepared_directory_change remove_source{
      *this, *from_parent_id, from_directory, from_name, std::nullopt};
    prepared_directory_change add_destination{
      *this, *to_parent_id, to_directory, to_name, source_id};
    remove_source.commit();
    add_destination.commit();
    if (replaced && *replaced != source_id) {
        collect_unreachable(*replaced);
    }
    return {};
}

runtime::result<void>
fake_file_system::sync_directory(const canonical_fake_path& path) {
    assert_current();
    auto selected_id = lookup(path);
    if (!selected_id) {
        return runtime::failure(selected_id.error());
    }
    auto selected = directory(path);
    if (!selected) {
        return runtime::failure(selected.error());
    }
    auto& state = **selected;

    struct sync_change final {
        std::string name;
        std::optional<fake_object_id> replacement;
        std::optional<fake_object_id> previous;
        bool inserted{false};
    };

    seastar::chunked_vector<sync_change> changes;
    changes.reserve(state.unsynced.size());
    seastar::chunked_vector<std::uint64_t> maybe_unreachable;
    maybe_unreachable.reserve(state.unsynced.size());
    std::int64_t path_delta = 0;
    for (const auto& [name, replacement] : state.unsynced) {
        const auto existing = state.durable.find(name);
        const std::optional<fake_object_id> previous
          = existing == state.durable.end()
              ? std::optional<fake_object_id>{}
              : std::optional<fake_object_id>{existing->second};
        if (previous && (!replacement || *previous != *replacement)) {
            maybe_unreachable.push_back(previous->value());
        }
        path_delta -= static_cast<std::int64_t>(name.size());
        if (replacement && !previous) {
            path_delta += static_cast<std::int64_t>(name.size());
        } else if (!replacement && previous) {
            path_delta -= static_cast<std::int64_t>(name.size());
        }
        changes.push_back(
          sync_change{
            .name = name,
            .replacement = replacement,
            .previous = previous,
          });
    }

    if (auto valid = validate_path_delta(path_delta); !valid) {
        return runtime::failure(valid.error());
    }
    std::size_t prepared = 0;
    try {
        for (; prepared < changes.size(); ++prepared) {
            auto& change = changes[prepared];
            if (change.replacement && !change.previous) {
                const auto [position, inserted] = state.durable.try_emplace(
                  change.name, *change.replacement);
                static_cast<void>(position);
                KWAQUE_INVARIANT(
                  fake_directory_transaction_invariant,
                  inserted,
                  "durable directory placeholder was not inserted");
                change.inserted = true;
            }
        }
    } catch (...) {
        for (std::size_t index = 0; index < prepared; ++index) {
            if (changes[index].inserted) {
                state.durable.erase(changes[index].name);
            }
        }
        throw;
    }

    for (const auto& change : changes) {
        if (
          change.previous
          && (!change.replacement || *change.previous != *change.replacement)) {
            --find_inode(*change.previous)->durable_links;
        }
        if (
          change.replacement
          && (!change.previous || *change.previous != *change.replacement)) {
            ++find_inode(*change.replacement)->durable_links;
        }
        if (change.replacement) {
            state.durable.find(change.name)->second = *change.replacement;
        } else if (change.previous) {
            state.durable.erase(change.name);
        }
    }
    state.unsynced.clear();
    clear_dirty(*find_inode(*selected_id));
    apply_path_delta(path_delta);
    collect_unreachable_from(std::move(maybe_unreachable));
    return {};
}

runtime::result<seastar::chunked_vector<fake_directory_entry>>
fake_file_system::list(const canonical_fake_path& path) const {
    assert_current();
    auto selected = directory(path);
    if (!selected) {
        return runtime::failure(selected.error());
    }
    seastar::chunked_vector<fake_directory_entry> entries;
    entries.reserve((*selected)->durable.size() + (*selected)->unsynced.size());
    auto durable = (*selected)->durable.begin();
    auto changed = (*selected)->unsynced.begin();
    while (durable != (*selected)->durable.end()
           || changed != (*selected)->unsynced.end()) {
        const bool take_durable
          = changed == (*selected)->unsynced.end()
            || (durable != (*selected)->durable.end()
                && unsigned_name_less{}(durable->first, changed->first));
        const bool take_changed
          = durable == (*selected)->durable.end()
            || (changed != (*selected)->unsynced.end()
                && unsigned_name_less{}(changed->first, durable->first));
        std::string_view name;
        std::optional<fake_object_id> id;
        if (take_durable) {
            name = durable->first;
            id = durable->second;
            ++durable;
        } else if (take_changed) {
            name = changed->first;
            id = changed->second;
            ++changed;
        } else {
            name = changed->first;
            id = changed->second;
            ++durable;
            ++changed;
        }
        if (id) {
            entries.push_back(
              fake_directory_entry{
                .name = std::string{name},
                .id = *id,
                .kind = find_inode(*id)->kind,
              });
        }
    }
    return runtime::result<seastar::chunked_vector<fake_directory_entry>>{
      std::move(entries)};
}

runtime::result<fake_file_system::regular_file_state*>
fake_file_system::regular_file(const canonical_fake_path& path) noexcept {
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    auto* selected = find_inode(*id);
    if (selected->kind != fake_file_kind::regular) {
        return runtime::failure(file_error(errc::is_a_directory));
    }
    return &std::get<regular_file_state>(selected->state);
}

runtime::result<const fake_file_system::regular_file_state*>
fake_file_system::regular_file(const canonical_fake_path& path) const noexcept {
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    const auto* selected = find_inode(*id);
    if (selected->kind != fake_file_kind::regular) {
        return runtime::failure(file_error(errc::is_a_directory));
    }
    return &std::get<regular_file_state>(selected->state);
}

runtime::result<fake_file_system::directory_state*>
fake_file_system::directory(const canonical_fake_path& path) noexcept {
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    auto* selected = find_inode(*id);
    if (selected->kind != fake_file_kind::directory) {
        return runtime::failure(file_error(errc::not_a_directory));
    }
    return &std::get<directory_state>(selected->state);
}

runtime::result<const fake_file_system::directory_state*>
fake_file_system::directory(const canonical_fake_path& path) const noexcept {
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    const auto* selected = find_inode(*id);
    if (selected->kind != fake_file_kind::directory) {
        return runtime::failure(file_error(errc::not_a_directory));
    }
    return &std::get<directory_state>(selected->state);
}

std::uint64_t
fake_file_system::retained_size(const regular_file_state& file) noexcept {
    return std::max(file.visible_size, file.durable_size);
}

runtime::result<void> fake_file_system::update_retained_capacity(
  std::uint64_t before, std::uint64_t after) noexcept {
    if (
      after > before
      && after - before
           > config_.logical_capacity.value() - retained_capacity_.value()) {
        return runtime::failure(file_error(errc::resource_exhausted));
    }
    retained_capacity_ = byte_count{
      retained_capacity_.value() - before + after};
    return {};
}

void fake_file_system::mark_dirty(inode& object) noexcept {
    if (object.crash_dirty) {
        return;
    }
    object.dirty_previous = 0;
    object.dirty_next = dirty_head_;
    if (dirty_head_ != 0) {
        find_inode(fake_object_id{dirty_head_})->dirty_previous
          = object.id.value();
    }
    dirty_head_ = object.id.value();
    object.crash_dirty = true;
}

void fake_file_system::clear_dirty(inode& object) noexcept {
    if (!object.crash_dirty) {
        return;
    }
    if (object.dirty_previous == 0) {
        dirty_head_ = object.dirty_next;
    } else {
        find_inode(fake_object_id{object.dirty_previous})->dirty_next
          = object.dirty_next;
    }
    if (object.dirty_next != 0) {
        find_inode(fake_object_id{object.dirty_next})->dirty_previous
          = object.dirty_previous;
    }
    object.crash_dirty = false;
    object.dirty_previous = 0;
    object.dirty_next = 0;
}

runtime::result<byte_count> fake_file_system::write(
  const canonical_fake_path& path,
  std::uint64_t position,
  std::span<const std::byte> bytes) {
    assert_current();
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    return write(*id, position, bytes);
}

runtime::result<byte_count> fake_file_system::write(
  fake_object_id id, std::uint64_t position, std::span<const std::byte> bytes) {
    assert_current();
    if (bytes.empty()) {
        return runtime::failure(file_error(errc::invalid_argument));
    }
    if (bytes.size() > std::numeric_limits<std::uint64_t>::max() - position) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    auto* object = find_inode(id);
    if (object == nullptr) {
        return runtime::failure(file_error(errc::not_found));
    }
    if (object->kind != fake_file_kind::regular) {
        return runtime::failure(file_error(errc::is_a_directory));
    }
    auto& file = std::get<regular_file_state>(object->state);
    const auto before = retained_size(file);
    const auto new_size = std::max(
      file.visible_size, position + static_cast<std::uint64_t>(bytes.size()));
    const auto after = std::max(new_size, file.durable_size);
    if (
      after > before
      && after - before
           > config_.logical_capacity.value() - retained_capacity_.value()) {
        return runtime::failure(file_error(errc::resource_exhausted));
    }

    struct page_update final {
        std::uint64_t index;
        page_pointer replacement;
        bool existed;
        bool needs_dirty_entry;
        bool inserted{false};
    };
    seastar::chunked_vector<page_update> updates;
    const auto first_page = position / fake_file_page_bytes;
    const auto final_page = (position + static_cast<std::uint64_t>(bytes.size())
                             - 1U)
                            / fake_file_page_bytes;
    updates.reserve(static_cast<std::size_t>(final_page - first_page + 1U));

    std::size_t copied = 0;
    for (auto page_index = first_page; page_index <= final_page; ++page_index) {
        page initial{};
        const auto existing = file.visible_pages.find(page_index);
        const bool existed = existing != file.visible_pages.end();
        const bool needs_dirty_entry = !existed;
        if (existed) {
            initial = *existing->second.bytes;
        } else if (
          page_index < file.cleared_from_page.value_or(
            std::numeric_limits<std::uint64_t>::max())) {
            const auto durable = file.durable_pages.find(page_index);
            if (durable != file.durable_pages.end()) {
                initial = *durable->second.bytes;
            }
        }
        auto replacement = seastar::make_lw_shared<page>(std::move(initial));
        const auto page_start = page_index * fake_file_page_bytes;
        const auto begin = position > page_start
                             ? static_cast<std::size_t>(position - page_start)
                             : 0U;
        const auto count = std::min(
          fake_file_page_bytes - begin, bytes.size() - copied);
        std::memcpy(replacement->data() + begin, bytes.data() + copied, count);
        copied += count;
        page_pointer immutable = replacement;
        updates.push_back(
          page_update{
            .index = page_index,
            .replacement = std::move(immutable),
            .existed = existed,
            .needs_dirty_entry = needs_dirty_entry,
          });
    }

    const auto previous_dirty_count = file.dirty_page_count;
    try {
        for (const auto& update : updates) {
            if (!update.needs_dirty_entry) {
                continue;
            }
            if (file.dirty_page_count < file.dirty_pages.size()) {
                file.dirty_pages[file.dirty_page_count] = update.index;
            } else {
                file.dirty_pages.push_back(update.index);
            }
            ++file.dirty_page_count;
        }
        for (auto& update : updates) {
            if (update.existed) {
                continue;
            }
            const auto [position, inserted] = file.visible_pages.try_emplace(
              update.index,
              page_state{.bytes = update.replacement, .dirty = true});
            static_cast<void>(position);
            KWAQUE_INVARIANT(
              fake_storage_transaction_invariant,
              inserted,
              "new visible page was already present");
            update.inserted = true;
        }
    } catch (...) {
        for (const auto& update : updates) {
            if (update.inserted) {
                file.visible_pages.erase(update.index);
            }
        }
        file.dirty_page_count = previous_dirty_count;
        throw;
    }

    static_cast<void>(update_retained_capacity(before, after));
    for (auto& update : updates) {
        if (!update.inserted) {
            auto& page = file.visible_pages.find(update.index)->second;
            page.bytes = std::move(update.replacement);
            page.dirty = true;
        }
    }
    file.visible_size = new_size;
    mark_dirty(*object);
    return byte_count{static_cast<std::uint64_t>(bytes.size())};
}

runtime::result<byte_count> fake_file_system::read(
  const canonical_fake_path& path,
  std::uint64_t position,
  std::span<std::byte> destination) const noexcept {
    assert_current();
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    return read(*id, position, destination);
}

runtime::result<byte_count> fake_file_system::read(
  fake_object_id id,
  std::uint64_t position,
  std::span<std::byte> destination) const noexcept {
    assert_current();
    const auto* object = find_inode(id);
    if (object == nullptr) {
        return runtime::failure(file_error(errc::not_found));
    }
    if (object->kind != fake_file_kind::regular) {
        return runtime::failure(file_error(errc::is_a_directory));
    }
    const auto& file = std::get<regular_file_state>(object->state);
    if (position >= file.visible_size || destination.empty()) {
        return byte_count{};
    }
    const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(
      destination.size(), file.visible_size - position));
    std::size_t copied = 0;
    while (copied < count) {
        const auto at = position + copied;
        const auto page_index = at / fake_file_page_bytes;
        const auto page_offset = static_cast<std::size_t>(
          at % fake_file_page_bytes);
        const auto span = std::min(
          fake_file_page_bytes - page_offset, count - copied);
        const auto visible = file.visible_pages.find(page_index);
        const auto durable = file.durable_pages.find(page_index);
        if (visible != file.visible_pages.end()) {
            std::memcpy(
              destination.data() + copied,
              visible->second.bytes->data() + page_offset,
              span);
        } else if (
          page_index < file.cleared_from_page.value_or(
            std::numeric_limits<std::uint64_t>::max())
          && durable != file.durable_pages.end()) {
            std::memcpy(
              destination.data() + copied,
              durable->second.bytes->data() + page_offset,
              span);
        } else {
            std::fill_n(destination.data() + copied, span, std::byte{});
        }
        copied += span;
    }
    return byte_count{count};
}

runtime::result<void> fake_file_system::truncate(
  const canonical_fake_path& path, std::uint64_t size) {
    assert_current();
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    return truncate(*id, size);
}

runtime::result<void>
fake_file_system::truncate(fake_object_id id, std::uint64_t size) {
    auto prepared = prepare_truncate(id, size);
    if (!prepared) {
        return runtime::failure(prepared.error());
    }
    commit_truncate(std::move(*prepared));
    return {};
}

runtime::result<fake_file_system::prepared_truncate>
fake_file_system::prepare_truncate(fake_object_id id, std::uint64_t size) {
    assert_current();
    auto* object = find_inode(id);
    if (object == nullptr) {
        return runtime::failure(file_error(errc::not_found));
    }
    if (object->kind != fake_file_kind::regular) {
        return runtime::failure(file_error(errc::is_a_directory));
    }
    auto& file = std::get<regular_file_state>(object->state);
    const auto previous_size = file.visible_size;
    const auto before = retained_size(file);
    const auto after = std::max(size, file.durable_size);
    if (
      after > before
      && after - before
           > config_.logical_capacity.value() - retained_capacity_.value()) {
        return runtime::failure(file_error(errc::resource_exhausted));
    }

    prepared_truncate prepared{
      .object = id,
      .size = size,
      .previous_size = previous_size,
      .retained_before = before,
      .retained_after = after,
      .kept_pages = page_count(size),
    };
    const auto tail_bytes = static_cast<std::size_t>(
      size % fake_file_page_bytes);
    if (size < file.visible_size && tail_bytes != 0) {
        const auto tail_index = prepared.kept_pages - 1U;
        page replacement{};
        const auto visible = file.visible_pages.find(tail_index);
        if (visible != file.visible_pages.end()) {
            replacement = *visible->second.bytes;
        } else if (
          tail_index < file.cleared_from_page.value_or(
            std::numeric_limits<std::uint64_t>::max())) {
            const auto durable = file.durable_pages.find(tail_index);
            if (durable != file.durable_pages.end()) {
                replacement = *durable->second.bytes;
            }
        }
        std::fill(
          replacement.begin() + static_cast<std::ptrdiff_t>(tail_bytes),
          replacement.end(),
          std::byte{});
        auto mutable_tail = seastar::make_lw_shared<page>(
          std::move(replacement));
        page_pointer immutable_tail = mutable_tail;
        prepared.tail = std::move(immutable_tail);
        prepared.insert_tail = visible == file.visible_pages.end();
    }

    if (prepared.tail && prepared.insert_tail) {
        file.visible_pages.reserve(file.visible_pages.size() + 1U);
        file.dirty_pages.reserve(file.dirty_page_count + 1U);
    }
    return prepared;
}

void fake_file_system::commit_truncate(prepared_truncate prepared) noexcept {
    assert_current();
    auto* object = find_inode(prepared.object);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      object != nullptr && object->kind == fake_file_kind::regular,
      "prepared truncate lost its regular file");
    auto& file = std::get<regular_file_state>(object->state);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      file.visible_size == prepared.previous_size
        && retained_size(file) == prepared.retained_before,
      "prepared truncate observed intervening file state");

    if (prepared.tail && prepared.insert_tail) {
        const auto tail_index = prepared.kept_pages - 1U;
        const auto [position, inserted] = file.visible_pages.try_emplace(
          tail_index, page_state{.bytes = *prepared.tail, .dirty = true});
        static_cast<void>(position);
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          inserted,
          "truncate tail override was already present");
        if (file.dirty_page_count < file.dirty_pages.size()) {
            file.dirty_pages[file.dirty_page_count] = tail_index;
        } else {
            file.dirty_pages.push_back(tail_index);
        }
        ++file.dirty_page_count;
    }

    const auto retained = update_retained_capacity(
      prepared.retained_before, prepared.retained_after);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      retained.has_value(),
      "prepared truncate lost retained-capacity admission");
    if (prepared.size < file.visible_size) {
        for (auto current = file.visible_pages.begin();
             current != file.visible_pages.end();) {
            if (current->first >= prepared.kept_pages) {
                current = file.visible_pages.erase(current);
            } else {
                ++current;
            }
        }
        file.cleared_from_page = std::min(
          file.cleared_from_page.value_or(prepared.kept_pages),
          prepared.kept_pages);
        if (prepared.tail) {
            const auto tail_index = prepared.kept_pages - 1U;
            const auto found = file.visible_pages.find(tail_index);
            KWAQUE_INVARIANT(
              fake_storage_transaction_invariant,
              found != file.visible_pages.end(),
              "truncate tail override disappeared before commit");
            found->second.bytes = std::move(*prepared.tail);
            found->second.dirty = true;
        }
    }
    file.visible_size = prepared.size;
    if (prepared.size != prepared.previous_size) {
        mark_dirty(*object);
    }
}

runtime::result<void> fake_file_system::flush(const canonical_fake_path& path) {
    assert_current();
    auto id = lookup(path);
    if (!id) {
        return runtime::failure(id.error());
    }
    return flush(*id);
}

runtime::result<void> fake_file_system::flush(fake_object_id id) {
    assert_current();
    auto* object = find_inode(id);
    if (object == nullptr) {
        return runtime::failure(file_error(errc::not_found));
    }
    if (object->kind != fake_file_kind::regular) {
        return runtime::failure(file_error(errc::is_a_directory));
    }
    auto& file = std::get<regular_file_state>(object->state);
    const auto before = retained_size(file);
    seastar::chunked_vector<std::uint64_t> inserted_pages;
    inserted_pages.reserve(file.dirty_page_count);
    try {
        for (std::size_t index = 0; index < file.dirty_page_count; ++index) {
            const auto page_index = file.dirty_pages[index];
            const auto visible = file.visible_pages.find(page_index);
            if (
              visible == file.visible_pages.end()
              || file.durable_pages.contains(page_index)) {
                continue;
            }
            const auto [position, inserted] = file.durable_pages.try_emplace(
              page_index,
              page_state{.bytes = visible->second.bytes, .dirty = false});
            static_cast<void>(position);
            KWAQUE_INVARIANT(
              fake_storage_transaction_invariant,
              inserted,
              "new durable page was already present");
            try {
                inserted_pages.push_back(page_index);
            } catch (...) {
                file.durable_pages.erase(page_index);
                throw;
            }
        }
    } catch (...) {
        for (const auto page_index : inserted_pages) {
            file.durable_pages.erase(page_index);
        }
        throw;
    }

    if (file.cleared_from_page) {
        for (auto current = file.durable_pages.begin();
             current != file.durable_pages.end();) {
            const auto visible = file.visible_pages.find(current->first);
            if (
              current->first >= *file.cleared_from_page
              && (visible == file.visible_pages.end() || !visible->second.dirty)) {
                current = file.durable_pages.erase(current);
            } else {
                ++current;
            }
        }
    }
    for (std::size_t index = 0; index < file.dirty_page_count; ++index) {
        const auto page_index = file.dirty_pages[index];
        const auto visible = file.visible_pages.find(page_index);
        if (visible == file.visible_pages.end()) {
            file.durable_pages.erase(page_index);
            continue;
        }
        auto& durable = file.durable_pages.find(page_index)->second;
        durable.bytes = visible->second.bytes;
        durable.dirty = false;
    }
    file.durable_size = file.visible_size;
    file.visible_pages.clear();
    file.dirty_page_count = 0;
    file.cleared_from_page.reset();
    clear_dirty(*object);
    static_cast<void>(update_retained_capacity(before, retained_size(file)));
    return {};
}

void fake_file_system::restore_durable_state() noexcept {
    assert_current();
    while (dirty_head_ != 0) {
        auto* object = find_inode(fake_object_id{dirty_head_});
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          object != nullptr && object->crash_dirty,
          "dirty fake inode list contains a missing object");
        clear_dirty(*object);
        if (object->kind == fake_file_kind::regular) {
            auto& file = std::get<regular_file_state>(object->state);
            const auto before = retained_size(file);
            file.visible_pages.clear();
            file.visible_size = file.durable_size;
            file.dirty_page_count = 0;
            file.cleared_from_page.reset();
            static_cast<void>(
              update_retained_capacity(before, file.durable_size));
            continue;
        }

        auto& directory = std::get<directory_state>(object->state);
        for (const auto& [name, replacement] : directory.unsynced) {
            const auto durable = directory.durable.find(name);
            const std::optional<fake_object_id> durable_child
              = durable == directory.durable.end()
                  ? std::optional<fake_object_id>{}
                  : std::optional<fake_object_id>{durable->second};
            if (
              replacement
              && (!durable_child || *replacement != *durable_child)) {
                auto* child = find_inode(*replacement);
                --child->visible_links;
            }
            if (
              durable_child
              && (!replacement || *replacement != *durable_child)) {
                ++find_inode(*durable_child)->visible_links;
            }
            retained_path_bytes_ -= name.size();
        }
        for (const auto& [name, replacement] : directory.unsynced) {
            static_cast<void>(name);
            if (replacement) {
                collect_unreachable(*replacement);
            }
        }
        directory.unsynced.clear();
    }
    invalidate_handles();
}

void fake_file_system::invalidate_handles() noexcept {
    assert_current();
    generation_ = generation_ == std::numeric_limits<std::uint64_t>::max()
                    ? 1U
                    : generation_ + 1U;
    for (const auto value : open_objects_) {
        const fake_object_id id{value};
        auto* object = find_inode(id);
        if (object == nullptr) {
            continue;
        }
        object->open_references = 0;
        collect_unreachable(id);
    }
    open_objects_.clear();
    open_handles_ = 0;
}

runtime::result<void>
fake_file_system::schedule_terminal(pending_operation& operation) noexcept {
    assert_current();
    if (operation.phase == pending_phase::terminal_scheduled) {
        return {};
    }
    if (operation.phase == pending_phase::discard_pending) {
        const auto* failure = scheduler_->trace_failure();
        return runtime::failure(
          failure != nullptr ? *failure : file_error(errc::replay_divergence));
    }
    if (
      operation.phase == pending_phase::queued
      || operation.phase == pending_phase::crash_apply_scheduled
      || operation.phase == pending_phase::partial_resize_apply_scheduled) {
        const auto canceled = scheduler_->cancel(operation.completion_event);
        if (!canceled) {
            operation.phase = pending_phase::discard_pending;
            return runtime::failure(canceled.error());
        }
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          *canceled,
          "fake terminal transition lost its queued event");
    } else {
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          operation.phase == pending_phase::parked,
          "fake terminal transition observed an invalid phase");
        const auto index = static_cast<std::size_t>(operation.kind);
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          parked_operations_ != 0 && parked_by_kind_[index] != 0,
          "fake terminal transition lost parked-operation accounting");
        --parked_operations_;
        --parked_by_kind_[index];
        operation_changed_.broadcast();
    }

    operation.phase = pending_phase::terminal_scheduled;
    operation.terminal_event.release();
    const auto id = operation.id;
    auto terminal = scheduler_->schedule(
      scheduler_->now(),
      event_priority::normal(),
      [this, id] noexcept { complete_terminal(id); },
      trace_event_descriptor{
        .kind = operation.trace_kind,
        .stable_id = id.value(),
        .result = static_cast<std::uint32_t>(errc::aborted),
        .effect = trace_action::operation_discarded,
      },
      event_cleanup_policy::invoke,
      std::move(operation.terminal_trace));
    if (!terminal) {
        operation.phase = pending_phase::discard_pending;
        return runtime::failure(terminal.error());
    }
    operation.completion_event = *terminal;
    return {};
}

runtime::result<void> fake_file_system::begin_crash(fake_operation_id active) {
    assert_current();
    auto& active_operation = pending_.at(active.value());

    pending_.copy_keys(pending_ids_);
    std::ranges::sort(pending_ids_);
    for (const auto value : pending_ids_) {
        if (value == active.value()) {
            continue;
        }
        auto* operation = pending_.find(value);
        if (operation == nullptr) {
            continue;
        }
        if (auto scheduled = schedule_terminal(*operation); !scheduled) {
            return runtime::failure(scheduled.error());
        }
    }

    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      active_operation.crash_event.active()
        && active_operation.crash_trace.active(),
      "crash operation has no reserved apply event");
    active_operation.phase = pending_phase::crash_apply_scheduled;
    active_operation.crash_event.release();
    auto applied = scheduler_->schedule(
      scheduler_->now(),
      event_priority::highest(),
      [this, active] noexcept { apply_crash(active); },
      trace_event_descriptor{
        .kind = trace_event_kind::filesystem,
        .domain = runtime::descriptor_for(active_operation.point)->id.value(),
        .stable_id = active.value(),
        .result = static_cast<std::uint32_t>(
          active_operation.kind == pending_kind::crash_control ? errc::success
                                                               : errc::aborted),
        .effect = trace_action::crash_applied,
      },
      event_cleanup_policy::invoke,
      std::move(active_operation.crash_trace));
    if (!applied) {
        active_operation.phase = pending_phase::queued;
        return runtime::failure(applied.error());
    }
    active_operation.completion_event = *applied;
    state_ = fake_file_system_state::crashing;
    operation_changed_.broadcast();
    return {};
}

runtime::result<void>
fake_file_system::begin_partial_resize(fake_operation_id active) {
    assert_current();
    auto& operation = pending_.at(active.value());
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      operation.kind == pending_kind::truncate
        && operation.fault.action() == runtime::fault_action::partial_resize
        && operation.truncate_commit.has_value()
        && operation.partial_resize_event.active()
        && operation.partial_resize_trace.active(),
      "partial resize lost its prepared commit or reservation");
    const auto* io = std::get_if<native_io_operation>(&operation.payload);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      io != nullptr,
      "partial resize lost its requested size");

    operation.phase = pending_phase::partial_resize_apply_scheduled;
    operation.partial_resize_event.release();
    auto applied = scheduler_->schedule(
      scheduler_->now(),
      event_priority::highest(),
      [this, active] noexcept { apply_partial_resize(active); },
      trace_event_descriptor{
        .kind = trace_event_kind::file,
        .domain = runtime::descriptor_for(
                    runtime::builtin_fault_point::file_truncate)
                    ->id.value(),
        .stable_id = active.value(),
        .coordinate_a = operation.fault_a,
        .coordinate_b = operation.fault_b,
        .value = io->position,
        .result = static_cast<std::uint8_t>(
                    runtime::fault_action::partial_resize)
                  | UINT32_C(0x100),
        .effect = trace_action::partial_resize_applied,
      },
      event_cleanup_policy::invoke,
      std::move(operation.partial_resize_trace));
    if (!applied) {
        operation.phase = pending_phase::queued;
        operation.truncate_commit.reset();
        return runtime::failure(applied.error());
    }
    operation.completion_event = *applied;
    return {};
}

void fake_file_system::complete_terminal(fake_operation_id id) noexcept {
    assert_current();
    auto* operation = pending_.find(id.value());
    if (operation == nullptr) {
        return;
    }
    if (scheduler_->discarding_failed_event()) [[unlikely]] {
        const auto* failure = scheduler_->trace_failure();
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          failure != nullptr,
          "discarded fake terminal event has no trace failure");
        discard_operation(*operation, *failure);
        return;
    }
    finish(*operation, runtime::failure(file_error(errc::aborted)), true);
}

void fake_file_system::apply_crash(fake_operation_id active) noexcept {
    assert_current();
    auto* operation = pending_.find(active.value());
    if (operation == nullptr) {
        return;
    }
    if (scheduler_->discarding_failed_event()) [[unlikely]] {
        const auto* failure = scheduler_->trace_failure();
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          failure != nullptr,
          "discarded fake crash event has no trace failure");
        discard_operation(*operation, *failure);
        return;
    }
    restore_durable_state();
    state_ = fake_file_system_state::open;
    operation_changed_.broadcast();
    if (operation->kind == pending_kind::crash_control) {
        finish(*operation, pending_value{std::monostate{}}, true);
    } else {
        finish(*operation, runtime::failure(file_error(errc::aborted)), true);
    }
}

void fake_file_system::apply_partial_resize(fake_operation_id active) noexcept {
    assert_current();
    auto* operation = pending_.find(active.value());
    if (operation == nullptr) {
        return;
    }
    if (scheduler_->discarding_failed_event()) [[unlikely]] {
        const auto* failure = scheduler_->trace_failure();
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          failure != nullptr,
          "discarded partial resize has no trace failure");
        discard_operation(*operation, *failure);
        return;
    }
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      operation->phase == pending_phase::partial_resize_apply_scheduled
        && operation->truncate_commit.has_value(),
      "partial resize apply lost its prepared transaction");
    auto prepared = std::move(*operation->truncate_commit);
    operation->truncate_commit.reset();
    commit_truncate(std::move(prepared));
    finish(*operation, runtime::failure(file_error(errc::io_failure)), true);
}

void fake_file_system::discard_operation(
  pending_operation& operation,
  const runtime::operation_error& failure) noexcept {
    assert_current();
    if (state_ == fake_file_system_state::stopping && !stop_failure_) {
        stop_failure_.emplace(failure);
    }
    finish(operation, runtime::failure(failure), true);
}

void fake_file_system::discard_remaining(
  const runtime::operation_error& failure) noexcept {
    assert_current();
    pending_.copy_keys(pending_ids_);
    std::ranges::sort(pending_ids_);
    for (const auto value : pending_ids_) {
        if (auto* operation = pending_.find(value); operation != nullptr) {
            discard_operation(*operation, failure);
        }
    }
}

void fake_file_system::finish_stop() noexcept {
    assert_current();
    KWAQUE_INVARIANT(
      fake_storage_drained_invariant,
      state_ == fake_file_system_state::stopping && pending_operations_ == 0,
      "fake filesystem stop completed with pending operations");
    state_ = fake_file_system_state::stopped;
    if (stop_failure_) {
        stop_done_->set_value(runtime::failure(*stop_failure_));
    } else {
        stop_done_->set_value(runtime::result<void>{});
    }
}

runtime::result<void>
fake_file_system::retain_open_reference(fake_object_id id) noexcept {
    assert_current();
    auto* selected = find_inode(id);
    if (selected == nullptr) {
        return runtime::failure(file_error(errc::not_found));
    }
    if (
      selected->open_references == std::numeric_limits<std::uint32_t>::max()) {
        return runtime::failure(file_error(errc::resource_exhausted));
    }
    if (selected->open_references == 0) {
        const auto [position, inserted] = open_objects_.insert(id.value());
        static_cast<void>(position);
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          inserted,
          "open fake inode was already tracked");
    }
    ++selected->open_references;
    return {};
}

runtime::result<seastar::file> fake_file_system::make_native_file_for_test(
  fake_object_id id, runtime::file_access access) {
    assert_current();
    const auto* object = find_inode(id);
    if (object == nullptr) {
        return runtime::failure(file_error(errc::not_found));
    }
    if (object->kind != fake_file_kind::regular) {
        return runtime::failure(file_error(errc::is_a_directory));
    }
    if (open_handles_ == config_.maximum_open_handles) {
        return runtime::failure(file_error(errc::queue_full));
    }
    auto handle = seastar::make_lw_shared<open_handle_state>();
    seastar::file native{seastar::make_shared<native_file_impl>(
      *this, id, access, generation_, handle)};
    if (auto retained = retain_open_reference(id); !retained) {
        return runtime::failure(retained.error());
    }
    ++open_handles_;
    handle->reference_owned = true;
    return native;
}

void fake_file_system::release_open_reference(fake_object_id id) {
    assert_current();
    auto* selected = find_inode(id);
    if (selected == nullptr || selected->open_references == 0) {
        return;
    }
    --selected->open_references;
    if (selected->open_references == 0) {
        const auto erased = open_objects_.erase(id.value());
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          erased != 0,
          "closed fake inode was not tracked");
    }
    collect_unreachable(id);
}

void fake_file_system::release_handle_reference(
  fake_object_id id, const seastar::lw_shared_ptr<open_handle_state>& handle) {
    assert_current();
    if (handle) {
        handle->assert_current();
    }
    if (!handle || !handle->reference_owned) {
        return;
    }
    handle->reference_owned = false;
    handle->lifecycle = handle_lifecycle::closed;
    release_open_reference(id);
    if (open_handles_ != 0) {
        --open_handles_;
    }
}

runtime::result<fake_operation_id>
fake_file_system::issue_operation_id() noexcept {
    assert_current();
    if (operation_ids_exhausted_) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    const fake_operation_id id{next_operation_id_};
    if (next_operation_id_ == std::numeric_limits<std::uint64_t>::max()) {
        operation_ids_exhausted_ = true;
    } else {
        ++next_operation_id_;
    }
    return id;
}

seastar::future<>
fake_file_system::wait_submitted(pending_kind kind, std::uint64_t count) {
    assert_current();
    const auto index = static_cast<std::size_t>(kind);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      kind != pending_kind::count && count != 0,
      "fake submission wait requires a valid kind and count");
    return operation_changed_.wait([this, index, count] {
        const auto submitted
          = index == static_cast<std::size_t>(pending_kind::read)
              ? submitted_by_kind_[index]
                  + submitted_by_kind_[static_cast<std::size_t>(
                    pending_kind::bulk_read)]
              : submitted_by_kind_[index];
        return submitted >= count || state_ != fake_file_system_state::open;
    });
}

seastar::future<>
fake_file_system::wait_parked(pending_kind kind, std::uint32_t count) {
    assert_current();
    const auto index = static_cast<std::size_t>(kind);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      kind != pending_kind::count && count != 0,
      "fake parked-operation wait requires a valid kind and count");
    return operation_changed_.wait([this, index, count] {
        const auto parked = index
                                == static_cast<std::size_t>(pending_kind::read)
                              ? parked_by_kind_[index]
                                  + parked_by_kind_[static_cast<std::size_t>(
                                    pending_kind::bulk_read)]
                              : parked_by_kind_[index];
        return parked >= count || state_ != fake_file_system_state::open;
    });
}

runtime::result<void> fake_file_system::validate_submission(
  pending_kind kind,
  byte_count retained_bytes,
  std::uint64_t retained_path_bytes,
  bool open_slot) const noexcept {
    if (state_ == fake_file_system_state::crashing) {
        return runtime::failure(file_error(errc::unavailable));
    }
    if (state_ != fake_file_system_state::open) {
        return runtime::failure(file_error(errc::closed));
    }
    if (scheduler_ == nullptr || faults_ == nullptr) {
        return runtime::failure(file_error(errc::unavailable));
    }
    if (retained_bytes > config_.maximum_operation_bytes) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    if (
      pending_operations_ == config_.maximum_pending_operations
      || retained_bytes.value()
           > config_.maximum_pending_bytes.value() - pending_bytes_.value()
      || retained_path_bytes > config_.maximum_retained_path_bytes.value()
                                 - retained_path_bytes_ - pending_path_bytes_
      || (open_slot && open_handles_ + pending_opens_ == config_.maximum_open_handles)
      || (is_read_operation(kind) && pending_reads_ == config_.maximum_pending_reads)
      || (is_write_operation(kind) && pending_writes_ == config_.maximum_pending_writes)) {
        return runtime::failure(file_error(errc::queue_full));
    }
    if (operation_ids_exhausted_) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    if (
      submitted_by_kind_[static_cast<std::size_t>(kind)]
      == std::numeric_limits<std::uint64_t>::max()) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    return {};
}

std::uint64_t fake_file_system::next_occurrence(
  runtime::builtin_fault_point point,
  std::optional<fake_object_id> object) const noexcept {
    const auto index = static_cast<std::size_t>(point);
    const auto current = object ? find_inode(*object)->occurrences[index]
                                : global_occurrences_[index];
    return current == std::numeric_limits<std::uint64_t>::max() ? 0
                                                                : current + 1U;
}

void fake_file_system::commit_occurrence(
  runtime::builtin_fault_point point,
  std::optional<fake_object_id> object) noexcept {
    const auto index = static_cast<std::size_t>(point);
    auto& occurrence = object ? find_inode(*object)->occurrences[index]
                              : global_occurrences_[index];
    ++occurrence;
}

bool fake_file_system::is_read_operation(pending_kind kind) noexcept {
    return kind == pending_kind::read || kind == pending_kind::bulk_read;
}

bool fake_file_system::is_write_operation(pending_kind kind) noexcept {
    return kind == pending_kind::write;
}

runtime::monotonic_duration fake_file_system::operation_latency(
  fake_operation_id operation, pending_kind kind) const noexcept {
    const auto minimum = is_read_operation(kind) ? config_.read_latency_min
                         : is_write_operation(kind)
                           ? config_.write_latency_min
                           : runtime::monotonic_duration{};
    const auto mean = is_read_operation(kind) ? config_.read_latency_mean
                      : is_write_operation(kind)
                        ? config_.write_latency_mean
                        : runtime::monotonic_duration{};
    if (mean.nanoseconds() == 0 || mean == minimum) {
        return minimum;
    }
    const auto doubled = mean.nanoseconds()
                             > (std::numeric_limits<std::uint64_t>::max() - 1U)
                                 / 2U
                           ? std::numeric_limits<std::uint64_t>::max()
                           : mean.nanoseconds() * 2U + 1U;
    const auto coordinate = random_coordinate::make(
      random_domain::storage_decision,
      operation.value(),
      static_cast<std::uint8_t>(kind) + 1U);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      coordinate.has_value(),
      "fake file latency produced an invalid coordinate");
    deterministic_random random{faults_->master_seed()};
    auto cursor = random.cursor(*coordinate, 0);
    const auto sampled = runtime::uniform_u64(cursor, doubled);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      sampled.has_value(),
      "fake file latency produced an invalid bound");
    return runtime::monotonic_duration{
      std::max(minimum.nanoseconds(), *sampled)};
}

runtime::result<runtime::monotonic_time> fake_file_system::completion_deadline(
  runtime::fault_decision fault,
  fake_operation_id operation,
  pending_kind kind) const noexcept {
    auto delay = config_.base_latency;
    if (
      const auto latency = delay.checked_add(
        operation_latency(operation, kind))) {
        delay = *latency;
    } else {
        return runtime::failure(file_error(errc::out_of_range));
    }
    if (const auto injected = fault.delay()) {
        const auto combined = delay.checked_add(*injected);
        if (!combined) {
            return runtime::failure(file_error(errc::out_of_range));
        }
        delay = *combined;
    }
    const auto deadline = scheduler_->now().checked_add(delay);
    if (!deadline || *deadline > scheduler_->limits().maximum_deadline()) {
        return runtime::failure(file_error(errc::out_of_range));
    }
    return *deadline;
}

seastar::future<runtime::result<fake_file_system::pending_value>>
fake_file_system::submit_file_operation(
  pending_kind kind,
  runtime::builtin_fault_point point,
  fake_object_id object,
  std::uint64_t position,
  std::uint64_t bytes,
  const char* source,
  char* destination,
  seastar::io_intent* intent,
  std::uint64_t generation,
  seastar::lw_shared_ptr<open_handle_state> handle) {
    assert_current();
    if (handle) {
        handle->assert_current();
    }
    const byte_count retained{bytes};
    if (
      generation != generation_
      || bytes > config_.maximum_operation_bytes.value()
      || bytes > std::numeric_limits<std::uint64_t>::max() - position) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(file_error(
            generation != generation_ ? errc::aborted : errc::out_of_range)));
    }
    if (auto valid = validate_submission(kind, retained, 0, false); !valid) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(valid.error()));
    }
    prepared_operation operation{
      fake_operation_id{next_operation_id_}, kind, point};
    operation.object = object;
    auto& io = operation.payload.emplace<native_io_operation>();
    io.position = position;
    io.requested_bytes = bytes;
    io.source = source;
    io.destination = destination;
    io.generation = generation;
    io.intent.emplace(intent);
    io.handle = std::move(handle);
    if (kind == pending_kind::bulk_read && bytes != 0) {
        io.snapshot = seastar::temporary_buffer<char>(
          static_cast<std::size_t>(bytes));
        io.destination = io.snapshot.get_write();
    } else if (bytes != 0 && (source != nullptr || destination != nullptr)) {
        const char* observed = source != nullptr ? source : destination;
        io.snapshot = seastar::temporary_buffer<char>(
          observed, static_cast<std::size_t>(bytes));
    }
    return submit(
      std::move(operation),
      runtime::fault_object_key::from_u64(object.value()),
      retained,
      trace_event_descriptor{
        .kind = trace_event_kind::file,
        .stable_id = next_operation_id_,
      });
}

seastar::future<runtime::result<fake_file_system::pending_value>>
fake_file_system::submit(
  prepared_operation owned_operation,
  runtime::fault_object_key object_key,
  byte_count retained_bytes,
  trace_event_descriptor descriptor) {
    assert_current();
    auto* operation = &owned_operation;
    const auto* metadata = std::get_if<metadata_operation>(&operation->payload);
    const auto* io = std::get_if<native_io_operation>(&operation->payload);
    const auto path_bytes
      = metadata == nullptr
          ? 0U
          : (metadata->path ? retained_path_size(*metadata->path) : 0U)
              + (metadata->destination_path ? retained_path_size(*metadata->destination_path) : 0U);
    const auto requested_bytes = io == nullptr ? 0U : io->requested_bytes;
    const auto io_position = io == nullptr ? 0U : io->position;
    if (
      auto valid = validate_submission(
        operation->kind, retained_bytes, path_bytes, operation->open_slot);
      !valid) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(valid.error()));
    }
    const auto occurrence = next_occurrence(
      operation->point, operation->object);
    if (occurrence == 0) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(file_error(errc::out_of_range)));
    }
    auto occurrence_value = runtime::fault_occurrence::make(occurrence);
    if (!occurrence_value) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(occurrence_value.error()));
    }
    auto prepared = faults_->prepare(
      runtime::fault_request{
        .point = runtime::descriptor_for(operation->point)->id,
        .occurrence = *occurrence_value,
        .object = object_key,
      });
    if (!prepared) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(prepared.error()));
    }
    operation->fault = prepared->preview();
    operation->configured_action = operation->fault.action();

    bool applicable = operation->fault.action() != runtime::fault_action::none;
    if (operation->fault.action() == runtime::fault_action::short_operation) {
        const auto cap = operation->fault.short_operation_bytes()->value();
        if (cap >= requested_bytes) {
            operation->fault = runtime::fault_decision{};
            applicable = false;
        }
    } else if (operation->fault.action() == runtime::fault_action::corrupt) {
        if (requested_bytes == 0) {
            operation->fault = runtime::fault_decision{};
            applicable = false;
        } else {
            operation->fault_a = *prepared->draw_bounded(requested_bytes);
            operation->fault_b = *prepared->draw_bounded(8);
        }
    } else if (operation->fault.action() == runtime::fault_action::torn_write) {
        if (requested_bytes <= 1) {
            operation->fault = runtime::fault_decision{};
            applicable = false;
        } else {
            operation->fault_a = *prepared->draw_bounded(requested_bytes - 1U)
                                 + 1U;
        }
    } else if (operation->fault.action() == runtime::fault_action::misdirect) {
        const auto* inode = operation->object ? find_inode(*operation->object)
                                              : nullptr;
        const auto* file = inode != nullptr
                               && inode->kind == fake_file_kind::regular
                             ? &std::get<regular_file_state>(inode->state)
                             : nullptr;
        const auto length = requested_bytes;
        const auto before = file != nullptr && io_position >= length
                              ? io_position - length + 1U
                              : 0U;
        const auto after_begin = io_position + length;
        const auto after = file != nullptr && file->visible_size >= length
                               && after_begin <= file->visible_size - length
                             ? file->visible_size - length - after_begin + 1U
                             : 0U;
        if (before + after == 0) {
            operation->fault = runtime::fault_decision{};
            applicable = false;
        } else {
            const auto selected = *prepared->draw_bounded(before + after);
            operation->fault_a = selected < before
                                   ? selected
                                   : after_begin + selected - before;
        }
    } else if (
      operation->fault.action() == runtime::fault_action::partial_resize) {
        const auto* inode = operation->object ? find_inode(*operation->object)
                                              : nullptr;
        const auto* file = inode != nullptr
                               && inode->kind == fake_file_kind::regular
                             ? &std::get<regular_file_state>(inode->state)
                             : nullptr;
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          operation->kind == pending_kind::truncate && file != nullptr,
          "partial resize was selected outside regular-file truncate");
        operation->fault_b = file->visible_size;
        const auto lower = std::min(file->visible_size, io_position);
        const auto upper = std::max(file->visible_size, io_position);
        if (upper - lower <= 1U) {
            applicable = false;
        } else {
            operation->fault_a = lower
                                 + *prepared->draw_bounded(upper - lower - 1U)
                                 + 1U;
        }
    }

    const auto deadline = completion_deadline(
      operation->fault, operation->id, operation->kind);
    if (!deadline) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(deadline.error()));
    }
    descriptor.domain = runtime::descriptor_for(operation->point)->id.value();
    descriptor.stable_id = operation->id.value();
    descriptor.coordinate_a = operation->fault_a;
    descriptor.coordinate_b = operation->fault_b;
    descriptor.value = prepared->draws_consumed();
    descriptor.result = static_cast<std::uint8_t>(operation->configured_action)
                        | ((applicable ? 1U : 2U) << 8U);
    if (
      auto available = scheduler_->can_schedule(
        *deadline, descriptor, event_cleanup_policy::invoke);
      !available) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(available.error()));
    }
    auto main_trace = scheduler_->reserve_trace(descriptor);
    auto terminal_event = scheduler_->reserve_event_slot();
    auto terminal_trace = scheduler_->reserve_trace(
      trace_event_descriptor{
        .kind = descriptor.kind,
        .stable_id = operation->id.value(),
        .effect = trace_action::operation_discarded,
      });
    if (!main_trace || !terminal_event || !terminal_trace) {
        const auto error = !main_trace       ? main_trace.error()
                           : !terminal_event ? terminal_event.error()
                                             : terminal_trace.error();
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(error));
    }
    operation->terminal_event = std::move(*terminal_event);
    operation->terminal_trace = std::move(*terminal_trace);
    if (
      operation->fault.action() == runtime::fault_action::partial_resize
      && applicable) {
        auto partial_resize_event = scheduler_->reserve_event_slot();
        auto partial_resize_trace = scheduler_->reserve_trace(
          trace_event_descriptor{
            .kind = trace_event_kind::file,
            .domain = runtime::descriptor_for(
                        runtime::builtin_fault_point::file_truncate)
                        ->id.value(),
            .stable_id = operation->id.value(),
            .coordinate_a = operation->fault_a,
            .coordinate_b = operation->fault_b,
            .value = io_position,
            .result = static_cast<std::uint8_t>(
                        runtime::fault_action::partial_resize)
                      | UINT32_C(0x100),
            .effect = trace_action::partial_resize_applied,
          });
        if (!partial_resize_event || !partial_resize_trace) {
            const auto error = !partial_resize_event
                                 ? partial_resize_event.error()
                                 : partial_resize_trace.error();
            return seastar::make_ready_future<runtime::result<pending_value>>(
              runtime::failure(error));
        }
        operation->partial_resize_event = std::move(*partial_resize_event);
        operation->partial_resize_trace = std::move(*partial_resize_trace);
    }
    if (
      operation->fault.action() == runtime::fault_action::crash
      || operation->kind == pending_kind::crash_control) {
        auto crash_event = scheduler_->reserve_event_slot();
        auto crash_trace = scheduler_->reserve_trace(
          trace_event_descriptor{
            .kind = trace_event_kind::filesystem,
            .domain = runtime::descriptor_for(operation->point)->id.value(),
            .stable_id = operation->id.value(),
            .result = static_cast<std::uint32_t>(
              operation->kind == pending_kind::crash_control ? errc::success
                                                             : errc::aborted),
            .effect = trace_action::crash_applied,
          });
        if (!crash_event || !crash_trace) {
            const auto error = !crash_event ? crash_event.error()
                                            : crash_trace.error();
            return seastar::make_ready_future<runtime::result<pending_value>>(
              runtime::failure(error));
        }
        operation->crash_event = std::move(*crash_event);
        operation->crash_trace = std::move(*crash_trace);
    }
    if (
      auto available = scheduler_->can_schedule(
        *deadline, descriptor, event_cleanup_policy::invoke);
      !available) {
        return seastar::make_ready_future<runtime::result<pending_value>>(
          runtime::failure(available.error()));
    }
    operation->accounted_bytes = retained_bytes;
    operation->accounted_path_bytes = path_bytes;
    operation->trace_kind = descriptor.kind;
    auto waiting = operation->completion.get_future();
    const auto id = operation->id;
    const auto submitted_kind = operation->kind;
    const auto [position, inserted] = pending_.try_emplace(
      id.value(), std::move(owned_operation));
    static_cast<void>(position);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      inserted,
      "pending fake operation ID was already present");

    const auto fault_committed = prepared->commit();
    if (!fault_committed) {
        auto failed = std::move(pending_.at(id.value()).completion);
        const auto erased = pending_.erase(id.value());
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          erased,
          "failed fake operation disappeared during rollback");
        failed.set_value(runtime::failure(fault_committed.error()));
        return waiting;
    }
    commit_occurrence(
      pending_.at(id.value()).point, pending_.at(id.value()).object);
    if (pending_.at(id.value()).object) {
        ++find_inode(*pending_.at(id.value()).object)->pending_references;
    }
    static_cast<void>(issue_operation_id());
    ++pending_operations_;
    pending_reads_ += is_read_operation(pending_.at(id.value()).kind);
    pending_writes_ += is_write_operation(pending_.at(id.value()).kind);
    pending_bytes_ = *pending_bytes_.checked_add(retained_bytes);
    pending_path_bytes_ += path_bytes;
    if (pending_.at(id.value()).open_slot) {
        ++pending_opens_;
    }
    const auto scheduled = scheduler_->schedule(
      *deadline,
      event_priority::normal(),
      [this, id] noexcept { complete(id); },
      descriptor,
      event_cleanup_policy::invoke,
      std::move(*main_trace));
    if (!scheduled) {
        auto& rejected = pending_.at(id.value());
        finish(rejected, runtime::failure(scheduled.error()), true);
    } else {
        pending_.at(id.value()).completion_event = *scheduled;
    }
    ++submitted_by_kind_[static_cast<std::size_t>(submitted_kind)];
    operation_changed_.broadcast();
    return waiting;
}

void fake_file_system::complete(fake_operation_id id) noexcept {
    assert_current();
    auto* found = pending_.find(id.value());
    if (found == nullptr) {
        return;
    }
    auto& operation = *found;
    if (scheduler_->discarding_failed_event()) [[unlikely]] {
        const auto* failure = scheduler_->trace_failure();
        discard_operation(
          operation,
          failure != nullptr ? *failure : file_error(errc::replay_divergence));
        return;
    }
    try {
        auto result = apply(operation);
        if (
          operation.phase == pending_phase::crash_apply_scheduled
          || operation.phase == pending_phase::partial_resize_apply_scheduled) {
            return;
        }
        const bool resolve = operation.fault.action()
                             != runtime::fault_action::drop_completion;
        finish(operation, std::move(result), resolve);
    } catch (...) {
        auto completion = std::move(operation.completion);
        const auto accounted_bytes = operation.accounted_bytes;
        const auto accounted_path_bytes = operation.accounted_path_bytes;
        const auto kind = operation.kind;
        const bool open_slot = operation.open_slot;
        const auto object = operation.object;
        const auto erased = pending_.erase(id.value());
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          erased,
          "failed fake operation disappeared during cleanup");
        --pending_operations_;
        pending_reads_ -= is_read_operation(kind);
        pending_writes_ -= is_write_operation(kind);
        pending_bytes_ = *pending_bytes_.checked_sub(accounted_bytes);
        pending_path_bytes_ -= accounted_path_bytes;
        if (open_slot) {
            --pending_opens_;
        }
        if (object) {
            --find_inode(*object)->pending_references;
            collect_unreachable(*object);
        }
        completion.set_exception(std::current_exception());
        if (
          state_ == fake_file_system_state::stopping
          && pending_operations_ == 0) {
            finish_stop();
        }
    }
}

runtime::result<runtime::file>
fake_file_system::apply_open(metadata_operation& metadata, bool& open_slot) {
    assert_current();
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      metadata.path.has_value(),
      "fake open completion has no canonical path");
    if (auto valid = metadata.open_options.validate(); !valid) {
        return runtime::failure(valid.error());
    }

    auto existing = lookup(*metadata.path);
    const bool creating = !existing;
    if (
      creating
      && (existing.error().code() != errc::not_found || !metadata.open_options.create)) {
        return runtime::failure(existing.error());
    }

    const fake_object_id id = existing ? *existing
                                       : fake_object_id{next_object_id_};
    auto* selected = existing ? find_inode(id) : nullptr;
    if (selected != nullptr) {
        if (selected->kind != fake_file_kind::regular) {
            return runtime::failure(file_error(errc::is_a_directory));
        }
        if (metadata.open_options.create && metadata.open_options.exclusive) {
            return runtime::failure(file_error(errc::already_exists));
        }
        if (
          selected->open_references
          == std::numeric_limits<std::uint32_t>::max()) {
            return runtime::failure(file_error(errc::resource_exhausted));
        }
    }
    if (open_handles_ == config_.maximum_open_handles) {
        return runtime::failure(file_error(errc::queue_full));
    }

    // Allocate every ownership block before create/truncate can mutate
    // namespace or file state. The native handle begins uncommitted, so
    // unwinding these owners cannot release a reference that was never made
    // visible. The concrete runtime handle is assembled only after commit
    // because its lifecycle requires an explicit close once constructed.
    runtime::operation_statistics_owner statistics;
    auto handle = seastar::make_lw_shared<open_handle_state>();
    seastar::file native{seastar::make_shared<native_file_impl>(
      *this, id, metadata.open_options.access, generation_, handle)};

    bool inserted_open_tracking = false;
    if (selected == nullptr || selected->open_references == 0) {
        const auto [position, inserted] = open_objects_.insert(id.value());
        static_cast<void>(position);
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          inserted,
          "newly opened fake inode was already tracked");
        inserted_open_tracking = true;
    } else {
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          open_objects_.contains(id.value()),
          "referenced fake inode was not tracked as open");
    }

    if (creating) {
        try {
            auto created = create(*metadata.path, fake_file_kind::regular);
            if (!created) {
                const auto erased = open_objects_.erase(id.value());
                KWAQUE_INVARIANT(
                  fake_storage_transaction_invariant,
                  erased != 0,
                  "failed fake open lost its prepared tracking entry");
                return runtime::failure(created.error());
            }
            KWAQUE_INVARIANT(
              fake_storage_transaction_invariant,
              *created == id,
              "fake open created an unexpected object ID");
        } catch (...) {
            if (inserted_open_tracking) {
                static_cast<void>(open_objects_.erase(id.value()));
            }
            throw;
        }
        selected = find_inode(id);
    }

    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      selected != nullptr && selected->kind == fake_file_kind::regular,
      "fake open commit lost its regular file");
    if (metadata.open_options.truncate) {
        auto truncated = truncate(id, 0);
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          truncated.has_value(),
          "validated fake open truncate failed during commit");
    }

    ++selected->open_references;
    ++open_handles_;
    handle->reference_owned = true;
    if (open_slot) {
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          pending_opens_ != 0,
          "fake open commit lost its pending-handle reservation");
        --pending_opens_;
        open_slot = false;
    }
    return runtime::file{
      std::move(native), runtime::file_io_limits{}, std::move(statistics)};
}

runtime::result<fake_file_system::pending_value>
fake_file_system::apply(pending_operation& operation) {
    assert_current();
    auto* io = std::get_if<native_io_operation>(&operation.payload);
    auto* metadata = std::get_if<metadata_operation>(&operation.payload);
    if (io != nullptr && io->generation != 0 && io->generation != generation_) {
        return runtime::failure(file_error(errc::aborted));
    }
    if (io != nullptr && io->intent) {
        static_cast<void>(io->intent->retrieve());
    }
    if (
      io != nullptr && io->requested_bytes != 0
      && operation.kind != pending_kind::bulk_read
      && (io->source != nullptr || io->destination != nullptr)) {
        const char* observed = io->source != nullptr ? io->source
                                                     : io->destination;
        const auto difference = std::mismatch(
          observed, observed + io->snapshot.size(), io->snapshot.get());
        KWAQUE_INVARIANT(
          fake_dma_buffer_invariant,
          difference.first == observed + io->snapshot.size(),
          "native DMA buffer changed before simulated completion");
    }
    if (operation.fault.action() == runtime::fault_action::error) {
        return runtime::failure(file_error(errc::fault_injected));
    }
    if (operation.fault.action() == runtime::fault_action::partial_resize) {
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          operation.kind == pending_kind::truncate,
          "partial resize reached a non-truncate completion");
        if (operation.fault_a == 0) {
            return runtime::failure(file_error(errc::io_failure));
        }
        auto prepared = prepare_truncate(*operation.object, operation.fault_a);
        if (!prepared) {
            return runtime::failure(prepared.error());
        }
        operation.truncate_commit.emplace(std::move(*prepared));
        if (auto started = begin_partial_resize(operation.id); !started) {
            operation.truncate_commit.reset();
            return runtime::failure(started.error());
        }
        return pending_value{std::monostate{}};
    }
    if (
      operation.fault.action() == runtime::fault_action::crash
      || operation.kind == pending_kind::crash_control) {
        if (auto started = begin_crash(operation.id); !started) {
            return runtime::failure(started.error());
        }
        return pending_value{std::monostate{}};
    }

    switch (operation.kind) {
    case pending_kind::open: {
        auto opened = apply_open(*metadata, operation.open_slot);
        if (!opened) {
            return runtime::failure(opened.error());
        }
        return pending_value{std::move(*opened)};
    }
    case pending_kind::exists: {
        auto existing = lookup(*metadata->path);
        if (!existing && existing.error().code() != errc::not_found) {
            return runtime::failure(existing.error());
        }
        return pending_value{existing.has_value()};
    }
    case pending_kind::stat: {
        auto id = lookup(*metadata->path);
        if (!id) {
            return runtime::failure(id.error());
        }
        const auto* object = find_inode(*id);
        const auto size
          = object->kind == fake_file_kind::regular
              ? std::get<regular_file_state>(object->state).visible_size
              : 0U;
        return pending_value{runtime::file_status{
          .kind = object->kind == fake_file_kind::regular
                    ? runtime::file_kind::regular
                    : runtime::file_kind::directory,
          .size = byte_count{size},
        }};
    }
    case pending_kind::list: {
        auto listed = list(*metadata->path);
        if (!listed) {
            return runtime::failure(listed.error());
        }
        seastar::chunked_vector<runtime::directory_entry> entries;
        std::uint64_t name_bytes = 0;
        entries.reserve(listed->size());
        for (auto& entry : *listed) {
            if (
              entries.size() == metadata->listing_limits.maximum_entries.value()
              || entry.name.size()
                   > metadata->listing_limits.maximum_name_bytes.value()
                       - name_bytes) {
                return runtime::failure(file_error(errc::resource_exhausted));
            }
            auto name = runtime::file_name::make(std::move(entry.name));
            if (!name) {
                return runtime::failure(name.error());
            }
            name_bytes += name->value().size();
            entries.push_back(
              runtime::directory_entry{
                .name = std::move(*name),
                .kind = entry.kind == fake_file_kind::regular
                          ? runtime::file_kind::regular
                          : runtime::file_kind::directory,
              });
        }
        auto listing = runtime::directory_listing::make(
          std::move(entries), metadata->listing_limits);
        if (!listing) {
            return runtime::failure(listing.error());
        }
        return pending_value{std::move(*listing)};
    }
    case pending_kind::create_directories: {
        std::vector<std::string> components = root_.components();
        seastar::chunked_vector<canonical_fake_path> missing;
        bool parent_missing = false;
        for (std::size_t index = root_.components().size();
             index < metadata->path->components().size();
             ++index) {
            components.push_back(metadata->path->components()[index]);
            std::string bytes{"/"};
            for (std::size_t component = 0; component < components.size();
                 ++component) {
                if (component != 0) {
                    bytes.push_back('/');
                }
                bytes.append(components[component]);
            }
            canonical_fake_path current{std::move(bytes), components};
            if (!parent_missing) {
                auto existing = lookup(current);
                if (existing) {
                    if (
                      find_inode(*existing)->kind
                      != fake_file_kind::directory) {
                        return runtime::failure(
                          file_error(errc::not_a_directory));
                    }
                    continue;
                }
                if (existing.error().code() != errc::not_found) {
                    return runtime::failure(existing.error());
                }
            }
            parent_missing = true;
            missing.push_back(std::move(current));
        }
        if (
          missing.size() > config_.maximum_objects - objects_.size()
          || (!missing.empty() && (object_ids_exhausted_ || missing.size() > std::numeric_limits<std::uint64_t>::max() - next_object_id_ + 1U))) {
            return runtime::failure(file_error(errc::resource_exhausted));
        }

        if (missing.empty()) {
            return pending_value{std::monostate{}};
        }

        const auto first_parent = lookup_parent(missing.front());
        if (!first_parent) {
            return runtime::failure(first_parent.error());
        }
        auto& first_parent_state = std::get<directory_state>(
          find_inode(*first_parent)->state);
        std::int64_t path_delta = directory_change_path_delta(
          first_parent_state, missing.front().components().back());
        for (std::size_t index = 1; index < missing.size(); ++index) {
            path_delta += static_cast<std::int64_t>(
              missing[index].components().back().size());
        }
        if (auto valid = validate_path_delta(path_delta); !valid) {
            return runtime::failure(valid.error());
        }

        seastar::chunked_vector<fake_object_id> staged_ids;
        staged_ids.reserve(missing.size());
        objects_.reserve(objects_.size() + missing.size());
        try {
            auto value = next_object_id_;
            for (std::size_t index = 0; index < missing.size(); ++index) {
                const fake_object_id id{value};
                const auto [position, inserted] = objects_.try_emplace(
                  id.value(),
                  std::make_unique<inode>(
                    id,
                    fake_file_kind::directory,
                    std::variant<regular_file_state, directory_state>{
                      std::in_place_type<directory_state>}));
                static_cast<void>(position);
                KWAQUE_INVARIANT(
                  fake_directory_transaction_invariant,
                  inserted,
                  "recursive directory object ID was already present");
                try {
                    staged_ids.push_back(id);
                } catch (...) {
                    objects_.erase(id.value());
                    throw;
                }
                if (value != std::numeric_limits<std::uint64_t>::max()) {
                    ++value;
                }
            }
        } catch (...) {
            for (const auto id : staged_ids) {
                objects_.erase(id.value());
            }
            throw;
        }

        seastar::chunked_vector<prepared_directory_change> changes;
        changes.reserve(missing.size());
        try {
            for (std::size_t index = 0; index < missing.size(); ++index) {
                auto& parent_state
                  = index == 0 ? first_parent_state
                               : std::get<directory_state>(
                                   find_inode(staged_ids[index - 1U])->state);
                changes.emplace_back(
                  *this,
                  index == 0 ? *first_parent : staged_ids[index - 1U],
                  parent_state,
                  missing[index].components().back(),
                  staged_ids[index]);
            }
        } catch (...) {
            changes.clear();
            for (const auto id : staged_ids) {
                objects_.erase(id.value());
            }
            throw;
        }

        for (auto& change : changes) {
            change.commit();
        }
        const auto final_id = staged_ids.back().value();
        if (final_id == std::numeric_limits<std::uint64_t>::max()) {
            object_ids_exhausted_ = true;
        } else {
            next_object_id_ = final_id + 1U;
        }
        return pending_value{std::monostate{}};
    }
    case pending_kind::remove_file: {
        auto result = remove(*metadata->path, fake_file_kind::regular);
        if (!result) {
            return runtime::failure(result.error());
        }
        return pending_value{std::monostate{}};
    }
    case pending_kind::remove_directory: {
        auto result = remove(*metadata->path, fake_file_kind::directory);
        if (!result) {
            return runtime::failure(result.error());
        }
        return pending_value{std::monostate{}};
    }
    case pending_kind::rename: {
        auto result = rename(*metadata->path, *metadata->destination_path);
        if (!result) {
            return runtime::failure(result.error());
        }
        return pending_value{std::monostate{}};
    }
    case pending_kind::sync_directory: {
        auto result = sync_directory(*metadata->path);
        if (!result) {
            return runtime::failure(result.error());
        }
        return pending_value{std::monostate{}};
    }
    case pending_kind::read:
    case pending_kind::bulk_read: {
        auto length = io->requested_bytes;
        if (const auto cap = operation.fault.short_operation_bytes()) {
            length = std::min(length, cap->value());
        }
        const auto position = operation.fault.action()
                                  == runtime::fault_action::misdirect
                                ? operation.fault_a
                                : io->position;
        auto read_result = read(
          *operation.object,
          position,
          std::as_writable_bytes(
            std::span{io->destination, static_cast<std::size_t>(length)}));
        if (!read_result) {
            return runtime::failure(read_result.error());
        }
        if (
          operation.fault.action() == runtime::fault_action::corrupt
          && read_result->value() != 0) {
            io->destination[operation.fault_a % read_result->value()]
              ^= static_cast<char>(1U << operation.fault_b);
        }
        if (operation.kind == pending_kind::bulk_read) {
            io->snapshot.trim(static_cast<std::size_t>(read_result->value()));
            auto output = seastar::temporary_buffer<std::uint8_t>::
              maybe_unsafe_from_deleter(
                reinterpret_cast<std::uint8_t*>(io->snapshot.get_write()),
                io->snapshot.size(),
                io->snapshot.release());
            return pending_value{std::move(output)};
        }
        return pending_value{*read_result};
    }
    case pending_kind::write: {
        auto length = io->requested_bytes;
        bool report_full = false;
        if (const auto cap = operation.fault.short_operation_bytes()) {
            length = std::min(length, cap->value());
        } else if (
          operation.fault.action() == runtime::fault_action::torn_write) {
            length = operation.fault_a;
            report_full = true;
        }
        const auto position = operation.fault.action()
                                  == runtime::fault_action::misdirect
                                ? operation.fault_a
                                : io->position;
        const char* source = io->source;
        if (operation.fault.action() == runtime::fault_action::corrupt) {
            io->snapshot.get_write()[operation.fault_a] ^= static_cast<char>(
              1U << operation.fault_b);
            source = io->snapshot.get();
        }
        auto written = write(
          *operation.object,
          position,
          std::as_bytes(std::span{source, static_cast<std::size_t>(length)}));
        if (!written) {
            return runtime::failure(written.error());
        }
        return pending_value{
          report_full ? byte_count{io->requested_bytes} : *written};
    }
    case pending_kind::flush: {
        auto result = flush(*operation.object);
        if (!result) {
            return runtime::failure(result.error());
        }
        return pending_value{std::monostate{}};
    }
    case pending_kind::truncate: {
        auto result = truncate(*operation.object, io->position);
        if (!result) {
            return runtime::failure(result.error());
        }
        return pending_value{std::monostate{}};
    }
    case pending_kind::size: {
        const auto* object = find_inode(*operation.object);
        if (object == nullptr) {
            return runtime::failure(file_error(errc::not_found));
        }
        return pending_value{
          std::get<regular_file_state>(object->state).visible_size};
    }
    case pending_kind::close:
        release_handle_reference(*operation.object, io->handle);
        return pending_value{std::monostate{}};
    case pending_kind::crash_control:
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          false,
          "fake crash control bypassed crash preparation");
    case pending_kind::count:
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          false,
          "fake operation used the pending-kind sentinel");
    }
    return runtime::failure(file_error(errc::invariant_violation));
}

void fake_file_system::finish(
  pending_operation& operation,
  runtime::result<pending_value> result,
  bool resolve) {
    assert_current();
    if (!resolve) {
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          operation.phase == pending_phase::queued,
          "fake operation reached the parked state from an invalid phase");
        operation.phase = pending_phase::parked;
        ++parked_operations_;
        ++parked_by_kind_[static_cast<std::size_t>(operation.kind)];
        operation_changed_.broadcast();
        return;
    }
    if (operation.phase == pending_phase::parked) {
        const auto index = static_cast<std::size_t>(operation.kind);
        KWAQUE_INVARIANT(
          fake_storage_transaction_invariant,
          parked_operations_ != 0 && parked_by_kind_[index] != 0,
          "fake completion lost parked-operation accounting");
        --parked_operations_;
        --parked_by_kind_[index];
        operation_changed_.broadcast();
    }
    const auto id = operation.id.value();
    auto completion = std::move(operation.completion);
    const auto accounted_bytes = operation.accounted_bytes;
    const auto accounted_path_bytes = operation.accounted_path_bytes;
    const auto kind = operation.kind;
    const bool open_slot = operation.open_slot;
    const auto object = operation.object;
    const auto erased = pending_.erase(id);
    KWAQUE_INVARIANT(
      fake_storage_transaction_invariant,
      erased,
      "completed fake operation disappeared during cleanup");
    --pending_operations_;
    pending_reads_ -= is_read_operation(kind);
    pending_writes_ -= is_write_operation(kind);
    pending_bytes_ = *pending_bytes_.checked_sub(accounted_bytes);
    pending_path_bytes_ -= accounted_path_bytes;
    if (open_slot) {
        --pending_opens_;
    }
    if (object) {
        --find_inode(*object)->pending_references;
        collect_unreachable(*object);
    }
    completion.set_value(std::move(result));
    if (
      state_ == fake_file_system_state::stopping && pending_operations_ == 0) {
        finish_stop();
    }
}

void fake_file_system::collect_unreachable(fake_object_id candidate) {
    collection_worklist_.reset();
    collection_worklist_.push_back(candidate.value());
    seastar::chunked_vector<std::uint64_t> none;
    collect_unreachable_from(std::move(none));
}

void fake_file_system::collect_unreachable_from(
  seastar::chunked_vector<std::uint64_t> pending) {
    if (!pending.empty()) {
        collection_worklist_.reset();
        for (const auto id : pending) {
            collection_worklist_.push_back(id);
        }
    }
    for (std::size_t index = 0; index < collection_worklist_.size(); ++index) {
        const auto found = objects_.find(collection_worklist_[index]);
        if (found == objects_.end()) {
            continue;
        }
        auto& object = *found->second;
        if (
          object.id.value() == 1 || object.visible_links != 0
          || object.durable_links != 0 || object.open_references != 0
          || object.pending_references != 0) {
            continue;
        }
        if (object.kind == fake_file_kind::regular) {
            retained_capacity_ = byte_count{
              retained_capacity_.value()
              - retained_size(std::get<regular_file_state>(object.state))};
        } else {
            const auto& directory = std::get<directory_state>(object.state);
            for (const auto& [name, child_id] : directory.durable) {
                retained_path_bytes_ -= name.size();
                auto* child = find_inode(child_id);
                --child->durable_links;
                if (!directory.unsynced.contains(name)) {
                    --child->visible_links;
                }
                if (
                  child->visible_links == 0 && child->durable_links == 0
                  && child->open_references == 0
                  && child->pending_references == 0) {
                    collection_worklist_.push_back(child_id.value());
                }
            }
            for (const auto& [name, replacement] : directory.unsynced) {
                retained_path_bytes_ -= name.size();
                if (!replacement) {
                    continue;
                }
                auto* child = find_inode(*replacement);
                --child->visible_links;
                if (
                  child->visible_links == 0 && child->durable_links == 0
                  && child->open_references == 0
                  && child->pending_references == 0) {
                    collection_worklist_.push_back(replacement->value());
                }
            }
        }
        clear_dirty(object);
        objects_.erase(found);
    }
}

} // namespace kwaque::simulation
