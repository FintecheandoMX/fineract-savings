/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.portfolio.savings.domain;

import lombok.extern.slf4j.Slf4j;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.fineract.accounting.journalentry.service.JournalEntryWritePlatformService;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.event.business.domain.savings.transaction.SavingsDepositBusinessEvent;
import org.apache.fineract.infrastructure.event.business.domain.savings.transaction.SavingsWithdrawalBusinessEvent;
import org.apache.fineract.infrastructure.event.business.service.BusinessEventNotifierService;
import org.apache.fineract.infrastructure.security.service.PlatformSecurityContext;
import org.apache.fineract.organisation.monetary.domain.ApplicationCurrencyRepositoryWrapper;
import org.apache.fineract.organisation.monetary.domain.Money;
import org.apache.fineract.organisation.monetary.domain.MoneyHelper;
import org.apache.fineract.portfolio.paymentdetail.domain.PaymentDetail;
import org.apache.fineract.portfolio.savings.SavingsAccountTransactionType;
import org.apache.fineract.portfolio.savings.SavingsTransactionBooleanValues;
import org.apache.fineract.portfolio.savings.data.SavingsAccountTransactionDTO;
import org.apache.fineract.portfolio.savings.exception.DepositAccountTransactionNotAllowedException;
import org.apache.fineract.portfolio.savings.service.SavingsAccountDomainService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class SavingsAccountDomainServiceJpa implements SavingsAccountDomainService {

    private final PlatformSecurityContext context;
    private final SavingsAccountRepositoryWrapper savingsAccountRepository;
    private final SavingsAccountTransactionRepository savingsAccountTransactionRepository;
    private final ApplicationCurrencyRepositoryWrapper applicationCurrencyRepositoryWrapper;
    private final JournalEntryWritePlatformService journalEntryWritePlatformService;
    private final ConfigurationDomainService configurationDomainService;
    private final DepositAccountOnHoldTransactionRepository depositAccountOnHoldTransactionRepository;
    private final BusinessEventNotifierService businessEventNotifierService;

    @Autowired
    public SavingsAccountDomainServiceJpa(final SavingsAccountRepositoryWrapper savingsAccountRepository,
            final SavingsAccountTransactionRepository savingsAccountTransactionRepository,
            final ApplicationCurrencyRepositoryWrapper applicationCurrencyRepositoryWrapper,
            final JournalEntryWritePlatformService journalEntryWritePlatformService,
            final ConfigurationDomainService configurationDomainService, final PlatformSecurityContext context,
            final DepositAccountOnHoldTransactionRepository depositAccountOnHoldTransactionRepository,
            final BusinessEventNotifierService businessEventNotifierService) {
        this.savingsAccountRepository = savingsAccountRepository;
        this.savingsAccountTransactionRepository = savingsAccountTransactionRepository;
        this.applicationCurrencyRepositoryWrapper = applicationCurrencyRepositoryWrapper;
        this.journalEntryWritePlatformService = journalEntryWritePlatformService;
        this.configurationDomainService = configurationDomainService;
        this.context = context;
        this.depositAccountOnHoldTransactionRepository = depositAccountOnHoldTransactionRepository;
        this.businessEventNotifierService = businessEventNotifierService;
    }

    @Transactional
    @Override
    public SavingsAccountTransaction handleWithdrawal(final SavingsAccount account, final DateTimeFormatter fmt,
            final LocalDate transactionDate, final BigDecimal transactionAmount, final PaymentDetail paymentDetail,
            final SavingsTransactionBooleanValues transactionBooleanValues, final boolean backdatedTxnsAllowedTill) {
        context.authenticatedUser();
        log.info("account.validateForAccountBlock()");
        account.validateForAccountBlock();
        log.info("account.validateForDebitBlock()");
        account.validateForDebitBlock();
        log.info("isSavingsInterestPostingAtCurrentPeriodEnd");
        final boolean isSavingsInterestPostingAtCurrentPeriodEnd = this.configurationDomainService.isSavingsInterestPostingAtCurrentPeriodEnd();
        log.info("relaxingDaysConfigForPivotDate");
        final Long relaxingDaysConfigForPivotDate = this.configurationDomainService.retrieveRelaxingDaysConfigForPivotDate();
        log.info("postReversals");
        final boolean postReversals = this.configurationDomainService.isReversalTransactionAllowed();
        log.info("financialYearBeginningMonth");
        final Integer financialYearBeginningMonth = this.configurationDomainService.retrieveFinancialYearBeginningMonth();
        log.info("if (transactionBooleanValues.isRegularTransaction() && !account.allowWithdrawal())");
        if (transactionBooleanValues.isRegularTransaction() && !account.allowWithdrawal()) {
            log.info("DepositAccountTransactionNotAllowedException");
            throw new DepositAccountTransactionNotAllowedException(account.getId(), "withdraw", account.depositAccountType());
        }
        log.info("existingTransactionIds");
        final Set<Long> existingTransactionIds = new HashSet<>();
        log.info("postInterestOnDate");
        final LocalDate postInterestOnDate = null;
        log.info("existingReversedTransactionIds");
        final Set<Long> existingReversedTransactionIds = new HashSet<>();
        log.info("(backdatedTxnsAllowedTill)");
        if (backdatedTxnsAllowedTill) {
            log.info("(updateTransactionDetailsWithPivotConfig)");
            updateTransactionDetailsWithPivotConfig(account, existingTransactionIds, existingReversedTransactionIds);
        } else {
            log.info("(updateExistingTransactionsDetails)");
            updateExistingTransactionsDetails(account, existingTransactionIds, existingReversedTransactionIds);
        }
        log.info("accountType");
        Integer accountType = null;
        log.info("transactionDTO");
        final SavingsAccountTransactionDTO transactionDTO = new SavingsAccountTransactionDTO(fmt, transactionDate, transactionAmount, paymentDetail, null, accountType);
        log.info("refNo");
        UUID refNo = UUID.randomUUID();
        log.info("SavingsAccountTransaction");
        final SavingsAccountTransaction withdrawal = account.withdraw(transactionDTO, transactionBooleanValues.isApplyWithdrawFee(), backdatedTxnsAllowedTill, relaxingDaysConfigForPivotDate, refNo.toString());
        log.info("MathContext");
        final MathContext mc = MathContext.DECIMAL64;
        log.info("LocalDate"); 
        final LocalDate today = DateUtils.getBusinessLocalDate();

        if (account.isBeforeLastPostingPeriod(transactionDate, backdatedTxnsAllowedTill)) {
            account.postInterest(mc, today, transactionBooleanValues.isInterestTransfer(), isSavingsInterestPostingAtCurrentPeriodEnd,
                    financialYearBeginningMonth, postInterestOnDate, backdatedTxnsAllowedTill, postReversals);
        } else {
            account.calculateInterestUsing(mc, today, transactionBooleanValues.isInterestTransfer(),
                    isSavingsInterestPostingAtCurrentPeriodEnd, financialYearBeginningMonth, postInterestOnDate, backdatedTxnsAllowedTill,
                    postReversals);
        }

        List<DepositAccountOnHoldTransaction> depositAccountOnHoldTransactions = null;
        if (account.getOnHoldFunds().compareTo(BigDecimal.ZERO) > 0) {
            depositAccountOnHoldTransactions = this.depositAccountOnHoldTransactionRepository
                    .findBySavingsAccountAndReversedFalseOrderByCreatedDateAsc(account);
        }

        account.validateAccountBalanceDoesNotBecomeNegative(transactionAmount, transactionBooleanValues.isExceptionForBalanceCheck(),
                depositAccountOnHoldTransactions, backdatedTxnsAllowedTill);

        saveTransactionToGenerateTransactionId(withdrawal);
        if (backdatedTxnsAllowedTill) {
            // Update transactions separately
            saveUpdatedTransactionsOfSavingsAccount(account.getSavingsAccountTransactionsWithPivotConfig());
        }
        this.savingsAccountRepository.save(account);

        postJournalEntries(account, existingTransactionIds, existingReversedTransactionIds, transactionBooleanValues.isAccountTransfer(),
                backdatedTxnsAllowedTill);

        businessEventNotifierService.notifyPostBusinessEvent(new SavingsWithdrawalBusinessEvent(withdrawal));
        return withdrawal;
    }

    @Transactional
    @Override
    public SavingsAccountTransaction handleDeposit(final SavingsAccount account, final DateTimeFormatter fmt,
            final LocalDate transactionDate, final BigDecimal transactionAmount, final PaymentDetail paymentDetail,
            final boolean isAccountTransfer, final boolean isRegularTransaction, final boolean backdatedTxnsAllowedTill) {
        final SavingsAccountTransactionType savingsAccountTransactionType = SavingsAccountTransactionType.DEPOSIT;
        return handleDeposit(account, fmt, transactionDate, transactionAmount, paymentDetail, isAccountTransfer, isRegularTransaction,
                savingsAccountTransactionType, backdatedTxnsAllowedTill);
    }

    private SavingsAccountTransaction handleDeposit(final SavingsAccount account, final DateTimeFormatter fmt,
            final LocalDate transactionDate, final BigDecimal transactionAmount, final PaymentDetail paymentDetail,
            final boolean isAccountTransfer, final boolean isRegularTransaction,
            final SavingsAccountTransactionType savingsAccountTransactionType, final boolean backdatedTxnsAllowedTill) {
        log.info("authenticatedUser"); 
        context.authenticatedUser();
        log.info("validateForAccountBlock"); 
        account.validateForAccountBlock();
        log.info("validateForCreditBlock"); 
        account.validateForCreditBlock();
        log.info("isSavingsInterestPostingAtCurrentPeriodEnd"); 
        // Global configurations
        final boolean isSavingsInterestPostingAtCurrentPeriodEnd = this.configurationDomainService.isSavingsInterestPostingAtCurrentPeriodEnd();
        log.info("financialYearBeginningMonth"); 
        final Integer financialYearBeginningMonth = this.configurationDomainService.retrieveFinancialYearBeginningMonth();
        log.info("relaxingDaysConfigForPivotDate"); 
        final Long relaxingDaysConfigForPivotDate = this.configurationDomainService.retrieveRelaxingDaysConfigForPivotDate();
        log.info("(isRegularTransaction && !account.allowDeposit())"); 
        if (isRegularTransaction && !account.allowDeposit()) {
            log.info("DepositAccountTransactionNotAllowedException"); 
            throw new DepositAccountTransactionNotAllowedException(account.getId(), "deposit", account.depositAccountType());
        }
        log.info("isInterestTransfer"); 
        boolean isInterestTransfer = false;
        log.info("existingTransactionIds"); 
        final Set<Long> existingTransactionIds = new HashSet<>();
        log.info("existingReversedTransactionIds"); 
        final Set<Long> existingReversedTransactionIds = new HashSet<>();
        log.info("backdatedTxnsAllowedTill"); 
        if (backdatedTxnsAllowedTill) {
            log.info("updateTransactionDetailsWithPivotConfig"); 
            updateTransactionDetailsWithPivotConfig(account, existingTransactionIds, existingReversedTransactionIds);
        } else {
            log.info("updateExistingTransactionsDetails"); 
            updateExistingTransactionsDetails(account, existingTransactionIds, existingReversedTransactionIds);
        }
        log.info("accountType"); 
        Integer accountType = null;
        log.info("transactionDTO"); 
        final SavingsAccountTransactionDTO transactionDTO = new SavingsAccountTransactionDTO(fmt, transactionDate, transactionAmount, paymentDetail, null, accountType);
        log.info("refNo"); 
        UUID refNo = UUID.randomUUID();
        log.info("deposit"); 
        final SavingsAccountTransaction deposit = account.deposit(transactionDTO, savingsAccountTransactionType, backdatedTxnsAllowedTill, relaxingDaysConfigForPivotDate, refNo.toString());
        log.info("postInterestOnDate"); 
        final LocalDate postInterestOnDate = null;
        log.info("mc"); 
        final MathContext mc = MathContext.DECIMAL64;
        log.info("today"); 
        final LocalDate today = DateUtils.getBusinessLocalDate();
        log.info("postReversals"); 
        boolean postReversals = this.configurationDomainService.isReversalTransactionAllowed();
        log.info("(account.isBeforeLastPostingPeriod(transactionDate, backdatedTxnsAllowedTill))"); 
        if (account.isBeforeLastPostingPeriod(transactionDate, backdatedTxnsAllowedTill)) {
            log.info("account.postInterest"); 
            account.postInterest(mc, today, isInterestTransfer, isSavingsInterestPostingAtCurrentPeriodEnd, financialYearBeginningMonth,
                    postInterestOnDate, backdatedTxnsAllowedTill, postReversals);
        } else {
            log.info("account.calculateInterestUsing"); 
            account.calculateInterestUsing(mc, today, isInterestTransfer, isSavingsInterestPostingAtCurrentPeriodEnd,
                    financialYearBeginningMonth, postInterestOnDate, backdatedTxnsAllowedTill, postReversals);
        }
        log.info("saveTransactionToGenerateTransactionId");
        saveTransactionToGenerateTransactionId(deposit);
        log.info("(backdatedTxnsAllowedTill)");
        if (backdatedTxnsAllowedTill) {
            // Update transactions separately
            log.info("saveUpdatedTransactionsOfSavingsAccount");
            saveUpdatedTransactionsOfSavingsAccount(account.getSavingsAccountTransactionsWithPivotConfig());
        }
        log.info("this.savingsAccountRepository.saveAndFlush(account)");
        this.savingsAccountRepository.saveAndFlush(account);
        log.info("postJournalEntries");
        postJournalEntries(account, existingTransactionIds, existingReversedTransactionIds, isAccountTransfer, backdatedTxnsAllowedTill);
        log.info("businessEventNotifierService.notifyPostBusinessEvent");
        businessEventNotifierService.notifyPostBusinessEvent(new SavingsDepositBusinessEvent(deposit));
        log.info("deposit");
        return deposit;
    }

    @Transactional
    @Override
    public SavingsAccountTransaction handleHold(final SavingsAccount account, BigDecimal amount, LocalDate transactionDate,
            Boolean lienAllowed) {
        return SavingsAccountTransaction.holdAmount(account, account.office(), null, transactionDate,
                Money.of(account.getCurrency(), amount), lienAllowed);
    }

    @Override
    public SavingsAccountTransaction handleDividendPayout(final SavingsAccount account, final LocalDate transactionDate,
            final BigDecimal transactionAmount, final boolean backdatedTxnsAllowedTill) {
        final DateTimeFormatter fmt = null;
        final PaymentDetail paymentDetail = null;
        final boolean isAccountTransfer = false;
        final boolean isRegularTransaction = true;
        final SavingsAccountTransactionType savingsAccountTransactionType = SavingsAccountTransactionType.DIVIDEND_PAYOUT;
        return handleDeposit(account, fmt, transactionDate, transactionAmount, paymentDetail, isAccountTransfer, isRegularTransaction,
                savingsAccountTransactionType, backdatedTxnsAllowedTill);
    }

    private void updateExistingTransactionsDetails(SavingsAccount account, Set<Long> existingTransactionIds,
            Set<Long> existingReversedTransactionIds) {
        existingTransactionIds.addAll(account.findExistingTransactionIds());
        existingReversedTransactionIds.addAll(account.findExistingReversedTransactionIds());
    }

    private Long saveTransactionToGenerateTransactionId(final SavingsAccountTransaction transaction) {
        this.savingsAccountTransactionRepository.saveAndFlush(transaction);
        return transaction.getId();
    }

    private void saveUpdatedTransactionsOfSavingsAccount(final List<SavingsAccountTransaction> savingsAccountTransactions) {
        this.savingsAccountTransactionRepository.saveAll(savingsAccountTransactions);
    }

    private void updateTransactionDetailsWithPivotConfig(final SavingsAccount account, Set<Long> existingTransactionIds,
            Set<Long> existingReversedTransactionIds) {
        existingTransactionIds.addAll(account.findCurrentTransactionIdsWithPivotDateConfig());
        existingReversedTransactionIds.addAll(account.findCurrentReversedTransactionIdsWithPivotDateConfig());
    }

    private void postJournalEntries(final SavingsAccount savingsAccount, final Set<Long> existingTransactionIds,
            final Set<Long> existingReversedTransactionIds, boolean isAccountTransfer, final boolean backdatedTxnsAllowedTill) {

        final Map<String, Object> accountingBridgeData = savingsAccount.deriveAccountingBridgeData(savingsAccount.getCurrency().getCode(),
                existingTransactionIds, existingReversedTransactionIds, isAccountTransfer, backdatedTxnsAllowedTill);
        this.journalEntryWritePlatformService.createJournalEntriesForSavings(accountingBridgeData);
    }

    @Transactional
    @Override
    public void postJournalEntries(final SavingsAccount account, final Set<Long> existingTransactionIds,
            final Set<Long> existingReversedTransactionIds, final boolean backdatedTxnsAllowedTill) {

        final boolean isAccountTransfer = false;
        postJournalEntries(account, existingTransactionIds, existingReversedTransactionIds, isAccountTransfer, backdatedTxnsAllowedTill);
    }

    @Override
    public SavingsAccountTransaction handleReversal(SavingsAccount account, List<SavingsAccountTransaction> savingsAccountTransactions,
            boolean backdatedTxnsAllowedTill) {

        final boolean isSavingsInterestPostingAtCurrentPeriodEnd = this.configurationDomainService
                .isSavingsInterestPostingAtCurrentPeriodEnd();
        final Integer financialYearBeginningMonth = this.configurationDomainService.retrieveFinancialYearBeginningMonth();
        final Long relaxingDaysConfigForPivotDate = this.configurationDomainService.retrieveRelaxingDaysConfigForPivotDate();
        final boolean postReversals = true;
        final Set<Long> existingTransactionIds = new HashSet<>();
        final Set<Long> existingReversedTransactionIds = new HashSet<>();

        if (backdatedTxnsAllowedTill) {
            updateTransactionDetailsWithPivotConfig(account, existingTransactionIds, existingReversedTransactionIds);
        } else {
            updateExistingTransactionsDetails(account, existingTransactionIds, existingReversedTransactionIds);
        }
        List<SavingsAccountTransaction> newTransactions = new ArrayList<>();
        SavingsAccountTransaction reversal = null;

        Set<SavingsAccountChargePaidBy> chargePaidBySet = null;
        for (SavingsAccountTransaction savingsAccountTransaction : savingsAccountTransactions) {
            reversal = SavingsAccountTransaction.reversal(savingsAccountTransaction);
            chargePaidBySet = savingsAccountTransaction.getSavingsAccountChargesPaid();
            reversal.getSavingsAccountChargesPaid().addAll(chargePaidBySet);
            account.undoTransaction(savingsAccountTransaction);
            if (postReversals) {
                newTransactions.add(reversal);
            }
        }

        boolean isInterestTransfer = false;
        LocalDate postInterestOnDate = null;
        final LocalDate today = DateUtils.getBusinessLocalDate();
        final MathContext mc = new MathContext(15, MoneyHelper.getRoundingMode());
        for (SavingsAccountTransaction savingsAccountTransaction : savingsAccountTransactions) {
            if (savingsAccountTransaction.isPostInterestCalculationRequired()
                    && account.isBeforeLastPostingPeriod(savingsAccountTransaction.getTransactionDate(), backdatedTxnsAllowedTill)) {

                account.postInterest(mc, today, isInterestTransfer, isSavingsInterestPostingAtCurrentPeriodEnd, financialYearBeginningMonth,
                        postInterestOnDate, backdatedTxnsAllowedTill, postReversals);
            } else {
                account.calculateInterestUsing(mc, today, isInterestTransfer, isSavingsInterestPostingAtCurrentPeriodEnd,
                        financialYearBeginningMonth, postInterestOnDate, backdatedTxnsAllowedTill, postReversals);
            }
            account.validatePivotDateTransaction(savingsAccountTransaction.getTransactionDate(), backdatedTxnsAllowedTill,
                    relaxingDaysConfigForPivotDate, "savingsaccount");
            account.validateAccountBalanceDoesNotBecomeNegativeMinimal(savingsAccountTransaction.getAmount(), false);
            account.activateAccountBasedOnBalance();
        }
        this.savingsAccountRepository.save(account);
        newTransactions.addAll(account.getSavingsAccountTransactionsWithPivotConfig());
        this.savingsAccountTransactionRepository.saveAll(newTransactions);
        postJournalEntries(account, existingTransactionIds, existingReversedTransactionIds, false, backdatedTxnsAllowedTill);

        return reversal;
    }
}
