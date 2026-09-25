#pragma once

#include "src/storage/tests/wal_group_commit_contract.h"

namespace kwaque::storage::testing::wal_commit_qualification_contract {
using store_contract::require;
using store_contract::take;

// Exhaust the delivery permutations of three admitted groups. The first two
// share a frozen barrier; the third must retain its own later certification.
template<
  runtime::monotonic_clock Clock,
  typename Backend,
  typename Owner,
  typename Driver>
seastar::future<> ordering(
  Backend& files,
  Owner& owner,
  const local_device_spec& spec,
  workload_budget& budget,
  Driver drive) {
    co_await wal_append_contract::with_writer(
      files,
      owner,
      spec,
      budget,
      drive,
      wal_writer_contract::configuration(),
      [&](auto& writer, auto& work) -> seastar::future<> {
          std::array<unsigned, 3> order{0, 1, 2};
          do {
              auto config = wal_group_commit_contract::slow_batch();
              config.target_members = 2;
              co_await wal_group_commit_contract::with_groups(
                writer,
                budget,
                drive,
                config,
                [&](auto& groups) -> seastar::future<> {
                    const auto before = writer.statistics().flush_calls;
                    const std::array records{assigned_wire()};
                    std::array<std::optional<wal_commit_ticket>, 3> tickets;
                    std::array<
                      std::optional<seastar::future<wal_commit_result>>,
                      3>
                      waiters;
                    for (unsigned i = 0; i != 2; ++i) {
                        auto input = co_await wal_append_contract::offer(
                          budget, spec.owner.cluster(), work, records);
                        tickets[i].emplace(take(
                          co_await groups.template submit<Clock>(
                            writer, std::move(input), work)));
                        waiters[i].emplace(take(tickets[i]->observe()));
                    }
                    auto capture = groups.capture(Clock::now());
                    require(
                      capture && capture->groups() == 2,
                      "cohort freeze lost membership");
                    auto extra = co_await wal_append_contract::offer(
                      budget, spec.owner.cluster(), work, records);
                    tickets[2].emplace(take(
                      co_await groups.template submit<Clock>(
                        writer, std::move(extra), work)));
                    waiters[2].emplace(take(tickets[2]->observe()));
                    auto duplicate = take(tickets[0]->observe());
                    take(groups.flush(writer, *capture));
                    take(groups.flush(writer, *capture));
                    auto proof = co_await drive.lifecycle(std::move(duplicate));
                    take(proof.failure().outcome());
                    require(
                      proof.receipt()
                        && proof.receipt()->boundary() == tickets[1]->boundary()
                        && !waiters[2]->available()
                        && writer.statistics().flush_calls == before + 1,
                      "same-cut join flushed twice or certified a later "
                      "arrival");
                    groups.force();
                    auto tail = groups.capture(Clock::now());
                    require(
                      tail && tail->groups() == 1, "later cohort disappeared");
                    take(groups.flush(writer, *tail));
                    for (auto i : order) {
                        auto done = co_await drive.lifecycle(
                          std::move(*waiters[i]));
                        waiters[i].reset();
                        take(done.failure().outcome());
                        require(
                          done.boundary() == tickets[i]->boundary()
                            && done.receipt()
                            && done.receipt()->boundary()
                                 == tickets[i < 2 ? 1U : 2U]->boundary(),
                          "delivery order changed the registered boundary or "
                          "covering receipt");
                    }
                    take(co_await drive.lifecycle(groups.close()));
                    require(
                      writer.statistics().flush_calls == before + 2
                        && groups.queued_groups() == 0,
                      "joined cleanup repeated a completed barrier");
                });
          } while (std::next_permutation(order.begin(), order.end()));
      });
}
} // namespace kwaque::storage::testing::wal_commit_qualification_contract
