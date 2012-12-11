using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using EasyNetQ.SystemMessages;

using MySql.Data.MySqlClient;

namespace EasyNetQ.Scheduler
{
    public interface IScheduleRepository
    {
        void Store(ScheduleMe scheduleMe);
        IList<ScheduleMe> GetPending();
        void Purge();
    }

    public class ScheduleRepository : IScheduleRepository
    {
        private const string insertSql = "uspAddNewMessageToScheduler";
        private const string selectSql = "uspGetNextBatchOfMessages";
        private const string purgeSql = "uspWorkItemsSelfPurge";
        private const string markForPurgeSql = "uspMarkWorkItemForPurge";

        private readonly ScheduleRepositoryConfiguration configuration;
        private readonly Func<DateTime> now; 

        public ScheduleRepository(ScheduleRepositoryConfiguration configuration, Func<DateTime> now)
        {
            this.configuration = configuration;
            this.now = now;
        }

        public void Store(ScheduleMe scheduleMe)
        {
            WithStoredProcedureCommand(
                insertSql,
                command =>
                    {
                        command.Parameters.Add(new MySqlParameter("@WakeTime", scheduleMe.WakeTime));
                        command.Parameters.Add(new MySqlParameter("@BindingKey", scheduleMe.BindingKey));
                        command.Parameters.Add(new MySqlParameter("@Message", scheduleMe.InnerMessage));

                        command.ExecuteNonQuery();
                    });
        }

        public IList<ScheduleMe> GetPending()
        {
            var scheduledMessages = new List<ScheduleMe>();
            var scheuldeMessageIds = new List<int>();

            WithStoredProcedureCommand(
                selectSql,
                command =>
                    {
                        command.Parameters.Add(new MySqlParameter("@WakeTime", now()));
                        command.Parameters.Add(new MySqlParameter("@rows", configuration.MaximumScheduleMessagesToReturn));

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var fileSize = reader.GetBytes(4, 0, null, 0, 0);
                                var rawData = new byte[fileSize];
                                reader.GetBytes(4, 0, rawData, 0, (int)fileSize);

                                scheduledMessages.Add(
                                    new ScheduleMe
                                        {
                                            WakeTime = reader.GetDateTime(2),
                                            BindingKey = reader.GetString(3),
                                            InnerMessage = rawData
                                        });

                                scheuldeMessageIds.Add(reader.GetInt32(0));
                            }
                        }
                    });

            MarkItemsForPurge(scheuldeMessageIds);

            return scheduledMessages;
        }

        public void MarkItemsForPurge(IEnumerable<int> scheuldeMessageIds)
        {
            // mark items for purge on a background thread.
            ThreadPool.QueueUserWorkItem(
                state => WithStoredProcedureCommand(
                    markForPurgeSql,
                    command =>
                        {
                            var purgeDate = now().AddDays(configuration.PurgeDelayDays);

                    command.Parameters.Add(new MySqlParameter("@purgeDate", purgeDate));

                    var idParameter = new MySqlParameter("@ID", SqlDbType.Int);
                    command.Parameters.Add(idParameter);

                    foreach (var scheduleMessageId in scheuldeMessageIds)
                    {
                        idParameter.Value = scheduleMessageId;
                        command.ExecuteNonQuery();
                    }
                }));
        }

        public void Purge()
        {
            WithStoredProcedureCommand(
                purgeSql,
                command =>
                    {
                        command.Parameters.Add(new MySqlParameter("@rows", configuration.PurgeBatchSize));
                        command.Parameters.Add(new MySqlParameter("@purgeDate", null));
                        command.ExecuteNonQuery();
                    });
        }

        private void WithStoredProcedureCommand(string storedProcedureName, Action<DbCommand> commandAction)
        {
            using (var connection = new MySqlConnection(configuration.ConnectionString))
            using (var command = new MySqlCommand(storedProcedureName, connection))
            {
                connection.Open();
                command.CommandType = CommandType.StoredProcedure;

                commandAction(command);
            }
        }
    }
}